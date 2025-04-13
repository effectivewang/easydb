package com.easydb.index;

import com.easydb.storage.Tuple;
import com.easydb.storage.TupleId;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.storage.transaction.Transaction;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * B-tree index implementation.
 * Similar to PostgreSQL's BTree index implementation.
 */
public class BTreeIndex implements Index {
    private final BTreeNode root;
    private final IndexMetadata metadata;
    private final int order;
    private final ReadWriteLock lock;
    private long numEntries;
    private long height;

    public BTreeIndex(IndexMetadata metadata) {
        this.metadata = metadata;
        this.order = 100;  // Typical B-tree order
        this.root = new BTreeNode(order, true);
        this.lock = new ReentrantReadWriteLock();
        this.numEntries = 0;
        this.height = 1;
    }

    @Override
    public void insert(Tuple tuple, Transaction txn) {
        lock.writeLock().lock();
        try {
            Comparable key = extractKey(tuple);
            TupleId tid = tuple.id();
            root.insert(key, tid);
            numEntries++;
            
            // Update statistics
            updateStatistics();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(Tuple tuple, Transaction txn) {
        lock.writeLock().lock();
        try {
            Comparable key = extractKey(tuple);
            root.delete(key);
            numEntries--;
            
            // Update statistics
            updateStatistics();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<TupleId> find(Object key, Transaction txn) {
        lock.readLock().lock();
        try {
            return root.find((Comparable) key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<TupleId> findRange(Object start, Object end, Transaction txn) {
        lock.readLock().lock();
        try {
            return root.findRange((Comparable) start, (Comparable) end);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public IndexMetadata getMetadata() {
        return metadata;
    }

    private Comparable extractKey(Tuple tuple) {
        List<String> indexColumns = metadata.columns();
        if (indexColumns.size() == 1) {
            return (Comparable) tuple.getValue(indexColumns.get(0));
        } else {
            // Handle composite keys
            return new CompositeKey(tuple, indexColumns);
        }
    }

    private void updateStatistics() {
        // Update index statistics
        height = calculateHeight(root);
        // TODO: Update other statistics like space usage, key distribution, etc.
    }

    private long calculateHeight(BTreeNode node) {
        if (node.isLeaf()) {
            return 1;
        }
        BTreeNode firstChild = (BTreeNode) node.getValues().get(0);
        return 1 + calculateHeight(firstChild);
    }

    /**
     * Represents a composite key for multi-column indexes.
     */
    private static class CompositeKey implements Comparable<CompositeKey> {
        private final List<Comparable> components;

        public CompositeKey(Tuple tuple, List<String> columns) {
            this.components = columns.stream()
                .map(col -> (Comparable) tuple.getValue(col))
                .toList();
        }

        @Override
        public int compareTo(CompositeKey other) {
            for (int i = 0; i < components.size(); i++) {
                int result = components.get(i).compareTo(other.components.get(i));
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CompositeKey)) return false;
            CompositeKey that = (CompositeKey) o;
            return components.equals(that.components);
        }

        @Override
        public int hashCode() {
            return components.hashCode();
        }
    }
} 