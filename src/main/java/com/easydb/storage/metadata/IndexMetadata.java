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
    Instant createdAt,
    long entryCount,
    long sizeInBytes
) {
    public enum IndexType {
        BTREE,
        HASH,
        BITMAP
    }

    public IndexMetadata(String indexName, String tableName, List<String> columnNames, boolean isUnique, IndexType type) {
        this(
            indexName,
            tableName,
            columnNames,
            isUnique,
            type,
            Instant.now(),
            0L,
            0L
        );
    }

    public IndexMetadata withStats(long newEntryCount, long newSizeInBytes) {
        return new IndexMetadata(
            indexName,
            tableName,
            columnNames,
            isUnique,
            type,
            createdAt,
            newEntryCount,
            newSizeInBytes
        );
    }
} 