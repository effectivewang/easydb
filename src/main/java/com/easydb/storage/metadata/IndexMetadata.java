package com.easydb.storage.metadata;

import java.util.List;
import java.time.Instant;


/**
 * Stores metadata about an index including its columns and statistics.
 */
public record IndexMetadata(
    String indexName,
    String tableName,
    List<String> columnNames,
    boolean isUnique,
    IndexType type,
    Instant createdAt
) {
    public IndexMetadata(String indexName, String tableName, List<String> columnNames, boolean isUnique, IndexType type) {
        this(
            indexName,
            tableName,
            columnNames,
            isUnique,
            type,
            Instant.now()
        );
    }

    public List<String> columnNames() {
        return columnNames;
    }

    
} 