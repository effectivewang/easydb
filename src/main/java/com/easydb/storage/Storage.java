package com.easydb.storage;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.storage.transaction.Transaction;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for storage operations in EasyDB.
 */
public interface Storage {
    /**
     * Creates a new table with the specified metadata.
     */
    CompletableFuture<Void> createTable(TableMetadata metadata);

    /**
     * Creates a new index with the specified metadata.
     */
    CompletableFuture<Void> createIndex(IndexMetadata metadata);

    /**
     * Inserts a new tuple into storage.
     */
    CompletableFuture<Void> insertTuple(Tuple tuple);

    /**
     * Finds tuples matching the specified conditions.
     */
    CompletableFuture<List<Tuple>> findTuples(String tableName, Map<String, Object> conditions);

    /**
     * Retrieves metadata for the specified table.
     */
    CompletableFuture<TableMetadata> getTableMetadata(String tableName);

    /**
     * Begins a new transaction.
     */
    CompletableFuture<Transaction> beginTransaction();
} 