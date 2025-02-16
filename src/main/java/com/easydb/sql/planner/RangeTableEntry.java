package com.easydb.sql.planner;

import com.easydb.storage.metadata.TableMetadata;
import java.util.List;
import java.util.ArrayList;

/**
 * Represents a table reference in the query, similar to PostgreSQL's RangeTblEntry.
 * Each table in the FROM clause gets a unique RTE.
 */
public class RangeTableEntry {
    private final String alias;          // Table alias (if any)
    private final String tableName;      // Actual table name
    private final TableMetadata metadata; // Table metadata
    private final int rteIndex;          // Unique index in the range table list

    public RangeTableEntry(String tableName, String alias, TableMetadata metadata, int rteIndex) {
        this.tableName = tableName;
        this.alias = alias;
        this.metadata = metadata;
        this.rteIndex = rteIndex;
    }

    public String getQualifiedName(String columnName) {
        return String.format("%s.%s", getDisplayName(), columnName);
    }

    public String getDisplayName() {
        return alias != null ? alias : tableName;
    }

    public String getAlias() {
        return alias;
    }

    public String getTableName() {
        return tableName;
    }

    public TableMetadata getMetadata() {
        return metadata;
    }

    public int getRteIndex() {
        return rteIndex;
    }

    public boolean hasColumn(String columnName) {
        return metadata.hasColumn(columnName);
    }

    public int getColumnIndex(String columnName) {
        return metadata.getColumnIndex(columnName);
    }
} 