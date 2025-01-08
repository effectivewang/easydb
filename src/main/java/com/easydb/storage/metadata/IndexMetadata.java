package com.easydb.storage.metadata;

import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Instant;
import com.easydb.core.TupleId;
import com.easydb.storage.metadata.IndexType;


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