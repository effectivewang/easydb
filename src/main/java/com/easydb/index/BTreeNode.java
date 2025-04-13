package com.easydb.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import com.easydb.storage.TupleId;

/**
 * Represents a node in the B-tree index.
 * Similar to PostgreSQL's BTNode structure.
 */
public class BTreeNode {
    private final long pageId;
    private final boolean isLeaf;
    private final List<Comparable> keys;
    private final List<Object> values;
    private BTreeNode parent;

    public BTreeNode(long pageId, boolean isLeaf) {
        this.pageId = pageId;
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
    }

    public void insert(Comparable key, Object value) {
        int index = findInsertIndex(key);
        keys.add(index, key);
        values.add(index, value);
    }

    public void delete(Comparable key) {
        int index = findKeyIndex(key);
        if (index >= 0) {
            keys.remove(index);
            values.remove(index);
        }
    }

    public void split(int mid, BTreeNode newNode) {
        // Move half of the keys and values to the new node
        for (int i = mid; i < keys.size(); i++) {
            newNode.keys.add(keys.get(i));
            newNode.values.add(values.get(i));
        }

        // Remove the moved keys and values from this node
        for (int i = keys.size() - 1; i >= mid; i--) {
            keys.remove(i);
            values.remove(i);
        }

        // Update parent references
        if (!isLeaf) {
            for (int i = 0; i < newNode.values.size(); i++) {
                BTreeNode child = (BTreeNode) newNode.values.get(i);
                child.setParent(newNode);
            }
        }
    }

    public void merge(BTreeNode other) {
        // Append all keys and values from the other node
        keys.addAll(other.keys);
        values.addAll(other.values);

        // Update parent references for non-leaf nodes
        if (!isLeaf) {
            for (Object value : other.values) {
                BTreeNode child = (BTreeNode) value;
                child.setParent(this);
            }
        }
    }

    private int findInsertIndex(Comparable key) {
        int left = 0;
        int right = keys.size() - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            int cmp = key.compareTo(keys.get(mid));
            
            if (cmp == 0) {
                return mid;
            } else if (cmp < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return left;
    }

    private int findKeyIndex(Comparable key) {
        int left = 0;
        int right = keys.size() - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            int cmp = key.compareTo(keys.get(mid));
            
            if (cmp == 0) {
                return mid;
            } else if (cmp < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return -1;
    }

    public long getPageId() {
        return pageId;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public List<Comparable> getKeys() {
        return keys;
    }

    public List<Object> getValues() {
        return values;
    }

    public BTreeNode getParent() {
        return parent;
    }

    public void setParent(BTreeNode parent) {
        this.parent = parent;
    }

    public boolean isFull(int maxKeys) {
        return keys.size() >= maxKeys;
    }

    public boolean isEmpty() {
        return keys.isEmpty();
    }

    public Comparable getMinKey() {
        return keys.isEmpty() ? null : keys.get(0);
    }

    public Comparable getMaxKey() {
        return keys.isEmpty() ? null : keys.get(keys.size() - 1);
    }
} 