package com.easydb.index;

import com.easydb.core.*;
import com.easydb.core.Transaction;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

/**
 * Lock-free Hash Index implementation
 */
public class HashIndex<K extends Comparable<K>, V> implements Index<K, V> {
    private final BloomFilter<K> bloomFilter;
    private final AtomicInteger size;
    
    // Segment the index into multiple sub-maps for better concurrency
    private static final int NUM_SEGMENTS = 64;
    private final ConcurrentHashMap<K, V>[] segments;
    // Transaction-specific changes
    private final ConcurrentHashMap<Long, Map<K, IndexOperation<V>>> uncommittedChanges;
    
    @SuppressWarnings("unchecked")
    public HashIndex(int expectedSize) {
        this.size = new AtomicInteger(0);
        
        // Initialize segments
        this.segments = new ConcurrentHashMap[NUM_SEGMENTS];
        for (int i = 0; i < NUM_SEGMENTS; i++) {
            segments[i] = new ConcurrentHashMap<>(expectedSize / NUM_SEGMENTS);
        }
        
        this.uncommittedChanges = new ConcurrentHashMap<>();
        // Configure Bloom filter
        int bloomSize = calculateOptimalBloomSize(expectedSize, 0.01);
        int numHashes = calculateOptimalNumHashes(expectedSize, bloomSize);
        this.bloomFilter = new BloomFilter<>(bloomSize, numHashes);
    }
    
    private int getSegment(K key) {
        return Math.abs(key.hashCode() % NUM_SEGMENTS);
    }
    
    @Override
    public CompletableFuture<Void> insert(K key, V value, Transaction txn) {
        return CompletableFuture.runAsync(() -> {
            if (txn != null) {
                // Store in uncommitted changes
                Map<K, IndexOperation<V>> txnChanges = uncommittedChanges
                    .computeIfAbsent(txn.getId(), k -> new ConcurrentHashMap<>());
                
                txnChanges.put(key, new IndexOperation<>(OperationType.INSERT, value));

                // Add cleanup action
                txn.addCleanupAction(() -> {
                    if (txn.isCommitted()) {
                        // On commit: remove from main index
                        int segment = getSegment(key);
                        segments[segment].put(key, value);
                        bloomFilter.add(key);
                    } else {
                        // On abort: remove from uncommitted changes
                        uncommittedChanges.get(txn.getId()).remove(key);
                    }
                });
            } else {
                // Direct insert
                segments[getSegment(key)].put(key, value);
                bloomFilter.add(key);
            }
        });
    }
    

    @Override
    public CompletableFuture<V> search(K key, Transaction txn) {
        return CompletableFuture.supplyAsync(() -> {
            if (txn != null) {
                // Check uncommitted changes first
                Map<K, IndexOperation<V>> txnChanges = uncommittedChanges.get(txn.getId());
                if (txnChanges != null) {
                    IndexOperation<V> op = txnChanges.get(key);
                    if (op != null) {
                        return op.type == OperationType.DELETE ? null : op.value;
                    }
                }

                // Check visibility based on isolation level
                switch (txn.getIsolationLevel()) {
                    case IsolationLevel.READ_UNCOMMITTED:
                        // See all uncommitted changes
                        for (Map<K, IndexOperation<V>> changes : uncommittedChanges.values()) {
                            IndexOperation<V> op = changes.get(key);
                            if (op != null) {
                                return op.type == OperationType.DELETE ? null : op.value;
                            }
                        }
                        break;
                    case IsolationLevel.READ_COMMITTED:
                        // Only see committed changes
                        break;
                    case IsolationLevel.REPEATABLE_READ:
                    case IsolationLevel.SERIALIZABLE:
                        // Add to read set
                        
                        break;
                }
            }

            // Fall back to main index
            if (!bloomFilter.mightContain(key)) {
                return null;
            }
            return segments[getSegment(key)].get(key);
        });
    }
    
    @Override
    public CompletableFuture<List<V>> range(K start, K end) {
        return CompletableFuture.supplyAsync(() -> {
            List<V> results = new ArrayList<>();
            for (ConcurrentHashMap<K, V> segment : segments) {
                segment.forEach((key, value) -> {
                    if (key.compareTo(start) >= 0 && key.compareTo(end) <= 0) {
                        results.add(value);
                    }
                });
            }
            return results;
        });
    }
    
    @Override
    public CompletableFuture<Void> delete(K key, Transaction txn) {
        return CompletableFuture.runAsync(() -> {
            if (txn != null) {
                // Mark as deleted in uncommitted changes
                Map<K, IndexOperation<V>> txnChanges = uncommittedChanges
                    .computeIfAbsent(txn.getId(), k -> new ConcurrentHashMap<>());
                
                txnChanges.put(key, new IndexOperation<>(OperationType.DELETE, null));
                
                // Add cleanup action
                txn.addCleanupAction(() -> {
                    if (txn.isCommitted()) {
                        // On commit: remove from main index
                        int segment = getSegment(key);
                        segments[segment].remove(key);
                    }
                    // Remove from uncommitted changes
                    uncommittedChanges.get(txn.getId()).remove(key);
                });
            } else {
                // Direct delete
                segments[getSegment(key)].remove(key);
            }
        });
    }
    
    @Override
    public CompletableFuture<Void> clear() {
        return CompletableFuture.runAsync(() -> {
            for (ConcurrentHashMap<K, V> segment : segments) {
                segment.clear();
            }
            size.set(0);
            // Create new Bloom filter since we can't clear the old one
            int bloomSize = calculateOptimalBloomSize(segments.length * 16, 0.01);
            int numHashes = calculateOptimalNumHashes(segments.length * 16, bloomSize);
            this.bloomFilter.clear();
        });
    }
    
    public int size() {
        return size.get();
    }
    
    private static int calculateOptimalBloomSize(int n, double p) {
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }
    
    private static int calculateOptimalNumHashes(int n, int m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

        // Helper classes for transaction management
    private enum OperationType { INSERT, DELETE }
    
    private static class IndexOperation<V> {
            final OperationType type;
            final V value;
            
            IndexOperation(OperationType type, V value) {
            this.type = type;
            this.value = value;
        }
    }
}
