package com.easydb.storage;

import java.util.concurrent.CompletableFuture;
import java.nio.ByteBuffer;

/**
 * Interface for storage operations in EasyDB.
 */
public interface Storage {
    /**
     * Write data to storage.
     *
     * @param key The key to write
     * @param value The value to write
     * @return A future that completes when the write is done
     */
    CompletableFuture<Void> write(ByteBuffer key, ByteBuffer value);

    /**
     * Read data from storage.
     *
     * @param key The key to read
     * @return A future that completes with the value
     */
    CompletableFuture<ByteBuffer> read(ByteBuffer key);

    /**
     * Delete data from storage.
     *
     * @param key The key to delete
     * @return A future that completes when the delete is done
     */
    CompletableFuture<Void> delete(ByteBuffer key);

    /**
     * Flush all pending writes to disk.
     *
     * @return A future that completes when the flush is done
     */
    CompletableFuture<Void> flush();

    /**
     * Close the storage.
     *
     * @return A future that completes when the close is done
     */
    CompletableFuture<Void> close();
} 