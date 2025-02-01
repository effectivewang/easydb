package com.easydb.storage;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
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
     * Inserts a new tuple into storage.
     */
    void insertTuple(Tuple tuple);

    /**
     * Finds tuples matching the specified conditions.
     */
    List<Tuple> findTuples(String tableName, Map<String, Object> conditions);


    /**
     * Retrieves metadata for the specified table.
     */
    TableMetadata getTableMetadata(String tableName);
} 