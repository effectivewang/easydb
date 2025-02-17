package com.easydb.storage;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.storage.transaction.Transaction;
import java.util.*;

/**
 * Interface for storage operations in EasyDB.
 */
public interface Storage {
    /**
     * Creates a new table with the specified metadata.
     */
    void createTable(TableMetadata metadata);

    /**
     * Creates a new index with the specified metadata.
     */
    void createIndex(IndexMetadata metadata);

    /**
     * Retrieves metadata for the specified table.
     */
    TableMetadata getTableMetadata(String tableName);

    /**
     * Inserts a new tuple into storage.
     */
    void insertTuple(Tuple tuple, Transaction txn);

    /**
     * Updates a tuple in storage.
     */
    void updateTuple(TupleId tupleId, List<Object> newValues, Transaction txn);

    /**
     * Deletes a tuple from storage.
     */
    void deleteTuple(TupleId tupleId, Transaction txn);

    /**
     * Finds tuples matching the specified conditions.
     */
    List<Tuple> scanTuples(String tableName, Map<String, Object> conditions, Transaction txn);

    /**
     * Retrieves a tuple by its ID.
     */
    Optional<Tuple> getTuple(TupleId tupleId, Transaction txn);

    /**
     * Retrieves tuples by their IDs.
     */
    List<Tuple> getTuples(List<TupleId> tupleIds, Transaction txn);
} 