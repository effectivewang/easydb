package com.easydb.storage.metadata;

import com.easydb.core.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Instant;
import java.util.stream.Collectors;
import com.easydb.storage.constraint.Constraint;

/**
 * Stores metadata about a table including its schema, indexes, and access patterns.
 */
public class TableMetadata {
    private final String tableName;
    private final List<Column> columns;
    private final Map<String, IndexMetadata> indexes;
    private final List<Constraint> constriants;
    private final Instant createdAt;
    private final Instant lastAccessedAt;
    private final long rowCount;
    private final long sizeInBytes;

    public TableMetadata(String tableName, List<Column> columns, Map<String, IndexMetadata> indexes,List<Constraint> constraints) {
        this(tableName, columns, indexes, constraints, Instant.now(), Instant.now(), 0, 0);
    }

    // Compact constructor for validation
    public TableMetadata(String tableName, List<Column> columns, Map<String, IndexMetadata> indexes, List<Constraint> constraints, Instant createdAt, Instant lastAccessedAt, long rowCount, long sizeInBytes)     {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns cannot be null or empty");
        }
        if (indexes == null) {
            indexes = new ConcurrentHashMap<>();
        }
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (lastAccessedAt == null) {
            lastAccessedAt = createdAt;
        }
        this.rowCount = rowCount;
        this.sizeInBytes = sizeInBytes; 
        this.columns = columns;
        this.indexes = indexes;
        this.createdAt = createdAt;
        this.lastAccessedAt = lastAccessedAt;
        this.tableName = tableName;
        this.constriants = constraints;
    }

    // Constructor with minimal parameters
    public TableMetadata(String tableName, List<Column> columns) {
        this(
            tableName,
            columns,
            new ConcurrentHashMap<>(),
            new ArrayList<>(),
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
            constriants,
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
            constriants,
            createdAt,
            lastAccessedAt,
            newRowCount,
            newSizeInBytes
        );
    }   

    public TableMetadata withIndex(IndexMetadata index) {
        Map<String, IndexMetadata> newIndexes = new ConcurrentHashMap<>(indexes);
        newIndexes.put(index.indexName(), index);

        return new TableMetadata(
            tableName,
            columns,
            newIndexes,
            constriants,
            createdAt,
            lastAccessedAt,
            rowCount,
            sizeInBytes
        );
    }

    public List<Class<?>> columnTypes() {
        return columns.stream()
            .map(column -> column.type().getJavaType())
            .collect(Collectors.toList());
    }

    public List<String> columnNames() {
        return columns.stream()
            .map(Column::name)
            .collect(Collectors.toList());
    }

    public Column getColumn(String name) {
        return columns.stream()
            .filter(column -> column.name().equals(name))
            .findFirst()
            .orElse(null);
    }
    public Column getColumn(int index) {
        return columns.get(index);
    }

    public int getColumnIndex(String columnName) {
        return columns.indexOf(getColumn(columnName));
    }
    
    
    public boolean hasColumn(String columnName) {
        return columns.stream()
            .anyMatch(column -> column.name().equals(columnName));
    }

    public String tableName() {
        return tableName;
    }

    public List<Column> columns() {
        return columns;
    }

    public List<Constraint> constraints() {
        return constriants;
    }

    public Map<String, IndexMetadata> indexes() {
        return indexes;
    }

    public boolean hasIndex(String columnName) {
        return indexes.containsKey(columnName);
    }

    public IndexMetadata getIndex(String columnName) {
        return indexes.get(columnName);
    }


} 
