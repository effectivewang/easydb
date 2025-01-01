package com.easydb.index;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.List;
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
    
    @SuppressWarnings("unchecked")
    public HashIndex(int expectedSize) {
        this.size = new AtomicInteger(0);
        
        // Initialize segments
        this.segments = new ConcurrentHashMap[NUM_SEGMENTS];
        for (int i = 0; i < NUM_SEGMENTS; i++) {
            segments[i] = new ConcurrentHashMap<>(expectedSize / NUM_SEGMENTS);
        }
        
        // Configure Bloom filter
        int bloomSize = calculateOptimalBloomSize(expectedSize, 0.01);
        int numHashes = calculateOptimalNumHashes(expectedSize, bloomSize);
        this.bloomFilter = new BloomFilter<>(bloomSize, numHashes);
    }
    
    private int getSegment(K key) {
        return Math.abs(key.hashCode() % NUM_SEGMENTS);
    }
    
    @Override
    public CompletableFuture<Void> insert(K key, V value) {
        return CompletableFuture.runAsync(() -> {
            int segment = getSegment(key);
            if (segments[segment].put(key, value) == null) {
                size.incrementAndGet();
            }
            bloomFilter.add(key);
        });
    }
    
    @Override
    public CompletableFuture<V> search(K key) {
        return CompletableFuture.supplyAsync(() -> {
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
    public CompletableFuture<Void> delete(K key) {
        return CompletableFuture.runAsync(() -> {
            int segment = getSegment(key);
            if (segments[segment].remove(key) != null) {
                size.decrementAndGet();
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
}