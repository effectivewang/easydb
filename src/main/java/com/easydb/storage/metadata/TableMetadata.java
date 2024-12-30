package com.easydb.storage.metadata;

import com.easydb.core.Column;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Instant;

/**
 * Stores metadata about a table including its schema, indexes, and access patterns.
 */
public record TableMetadata(
    String tableName,
    List<Column> columns,
    Map<String, IndexMetadata> indexes,
    Instant createdAt,
    Instant lastAccessedAt,
    long rowCount,
    long sizeInBytes
) {
    public TableMetadata(String tableName, List<Column> columns) {
        this(
            tableName,
            columns,
            new ConcurrentHashMap<>(),
            Instant.now(),
            Instant.now(),
            0L,
            0L
        );
    }

    public TableMetadata withLastAccessed() {
        return new TableMetadata(
            tableName,
            columns,
            indexes,
            createdAt,
            Instant.now(),
            rowCount,
            sizeInBytes
        );
    }

    public TableMetadata withRowCountAndSize(long newRowCount, long newSizeInBytes) {
        return new TableMetadata(
            tableName,
            columns,
            indexes,
            createdAt,
            lastAccessedAt,
            newRowCount,
            newSizeInBytes
        );
    }
} 