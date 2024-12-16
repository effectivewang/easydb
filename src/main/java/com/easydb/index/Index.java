package com.easydb.index;

import java.util.concurrent.CompletableFuture;
import java.util.List;

/**
 * Interface for index operations in EasyDB.
 */
public interface Index<K extends Comparable<K>, V> {
    /**
     * Insert a key-value pair into the index.
     *
     * @param key The key to insert
     * @param value The value to insert
     * @return A future that completes when the insertion is done
     */
    CompletableFuture<Void> insert(K key, V value);

    /**
     * Search for a key in the index.
     *
     * @param key The key to search for
     * @return A future that completes with the value
     */
    CompletableFuture<V> search(K key);

    /**
     * Range search in the index.
     *
     * @param start The start key (inclusive)
     * @param end The end key (exclusive)
     * @return A future that completes with the list of values in the range
     */
    CompletableFuture<List<V>> range(K start, K end);

    /**
     * Delete a key from the index.
     *
     * @param key The key to delete
     * @return A future that completes when the deletion is done
     */
    CompletableFuture<Void> delete(K key);

    /**
     * Clear the index.
     *
     * @return A future that completes when the clear is done
     */
    CompletableFuture<Void> clear();
} 