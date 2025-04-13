package com.easydb.index;

import com.easydb.storage.TupleId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;

/**
 * Implements B-tree index scanning.
 * Similar to PostgreSQL's BTScan structure.
 */
public class BTreeScan {
    private final BTreeIndex index;
    private final Comparable startKey;
    private final Comparable endKey;
    private final boolean includeStart;
    private final boolean includeEnd;
    private final Stack<BTreeNode> nodeStack;
    private final Stack<Integer> keyStack;
    private BTreeNode currentNode;
    private int currentKeyIndex;
    private boolean initialized;

    public BTreeScan(BTreeIndex index, Comparable startKey, Comparable endKey,
                    boolean includeStart, boolean includeEnd) {
        this.index = index;
        this.startKey = startKey;
        this.endKey = endKey;
        this.includeStart = includeStart;
        this.includeEnd = includeEnd;
        this.nodeStack = new Stack<>();
        this.keyStack = new Stack<>();
        this.initialized = false;
    }

    public void initialize() {
        if (initialized) {
            return;
        }

        // Find the starting leaf node
        currentNode = index.getRoot();
        while (!currentNode.isLeaf()) {
            int childIndex = findChildIndex(currentNode, startKey);
            nodeStack.push(currentNode);
            keyStack.push(childIndex);
            currentNode = (BTreeNode) currentNode.getValues().get(childIndex);
        }

        // Find the starting key in the leaf node
        currentKeyIndex = findKeyIndex(currentNode, startKey);
        if (currentKeyIndex >= currentNode.getKeys().size()) {
            currentKeyIndex = 0;
            moveToNextLeaf();
        }

        initialized = true;
    }

    public TupleId getNext() {
        if (!initialized) {
            initialize();
        }

        while (currentNode != null) {
            if (currentKeyIndex < currentNode.getKeys().size()) {
                Comparable key = currentNode.getKeys().get(currentKeyIndex);
                
                // Check if we've reached the end of the range
                if (endKey != null) {
                    int cmp = key.compareTo(endKey);
                    if (cmp > 0 || (cmp == 0 && !includeEnd)) {
                        return null;
                    }
                }

                // Check if we should include this key
                if (startKey != null) {
                    int cmp = key.compareTo(startKey);
                    if (cmp < 0 || (cmp == 0 && !includeStart)) {
                        currentKeyIndex++;
                        continue;
                    }
                }

                TupleId tupleId = (TupleId) currentNode.getValues().get(currentKeyIndex);
                currentKeyIndex++;
                return tupleId;
            } else {
                if (!moveToNextLeaf()) {
                    return null;
                }
            }
        }

        return null;
    }

    private boolean moveToNextLeaf() {
        while (!nodeStack.isEmpty()) {
            currentNode = nodeStack.pop();
            currentKeyIndex = keyStack.pop();

            if (currentKeyIndex < currentNode.getKeys().size()) {
                currentKeyIndex++;
                currentNode = (BTreeNode) currentNode.getValues().get(currentKeyIndex);
                while (!currentNode.isLeaf()) {
                    nodeStack.push(currentNode);
                    keyStack.push(0);
                    currentNode = (BTreeNode) currentNode.getValues().get(0);
                }
                currentKeyIndex = 0;
                return true;
            }
        }
        currentNode = null;
        return false;
    }

    private int findChildIndex(BTreeNode node, Comparable key) {
        List<Comparable> keys = node.getKeys();
        int left = 0;
        int right = keys.size() - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            int cmp = key.compareTo(keys.get(mid));
            
            if (cmp < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return left;
    }

    private int findKeyIndex(BTreeNode node, Comparable key) {
        List<Comparable> keys = node.getKeys();
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

    public void reset() {
        initialized = false;
        nodeStack.clear();
        keyStack.clear();
        currentNode = null;
        currentKeyIndex = 0;
    }
} 