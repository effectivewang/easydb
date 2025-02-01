package com.easydb.storage.metadata;

import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Instant;
import com.easydb.storage.TupleId;
import com.easydb.index.IndexType;


/**
 * Stores metadata about an index including its columns and statistics.
 */
public class IndexMetadata {
    private final String indexName;
    private final String tableName;
    private final List<String> columnNames;
    private final boolean isUnique;
    private final IndexType type;
    private final Instant createdAt;

    public IndexMetadata(String indexName, String tableName, List<String> columnNames, boolean isUnique, IndexType type) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.isUnique = isUnique;
        this.type = type;
        this.createdAt = Instant.now();
    }

    public List<String> columnNames() {
        return columnNames;
    }

    public String tableName() {
        return tableName;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public IndexType type() {
        return type;
    }
    
    public String indexName() {
        return indexName;
    }

    public Instant createdAt() {
        return createdAt;
    }
} 