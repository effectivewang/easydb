package com.easydb.storage;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.storage.transaction.Transaction;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory implementation of the storage engine.
 */
public class InMemoryStorage implements Storage {
    private final Map<String, TableMetadata> tables;
    private final Map<String, Map<TupleId, Tuple>> tuples;
    private final Map<String, Map<String, Map<String, Set<TupleId>>>> indexes;

    public InMemoryStorage() {
        this.tables = new ConcurrentHashMap<>();
        this.tuples = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
    }

    public CompletableFuture<Void> createTable(TableMetadata metadata) {
        return CompletableFuture.runAsync(() -> {
            tables.put(metadata.tableName(), metadata);
            tuples.put(metadata.tableName(), new ConcurrentHashMap<>());
            indexes.put(metadata.tableName(), new ConcurrentHashMap<>());
        });
    }

    public CompletableFuture<Void> createIndex(IndexMetadata metadata) {
        return CompletableFuture.runAsync(() -> {
            String tableName = metadata.tableName();
            String indexName = metadata.indexName();
            TableMetadata tableMetadata = tables.get(tableName);
            
            // Create index map
            Map<String, Set<TupleId>> indexMap = new ConcurrentHashMap<>();
            
            // Index existing tuples
            Map<TupleId, Tuple> tableTuples = tuples.get(tableName);
            List<Class<?>> columnTypes = tableMetadata.columns().stream()
                .map(column -> column.type().getJavaType())
                .collect(Collectors.toList());

            for (Map.Entry<TupleId, Tuple> entry : tableTuples.entrySet()) {
                List<Object> values = entry.getValue().getValues(columnTypes);
                String indexKey = buildIndexKey(tableName, metadata.columnNames(), values);
                indexMap.computeIfAbsent(indexKey, k -> new HashSet<>()).add(entry.getKey());
            }

            Map<String, Map<String, Set<TupleId>>> tableIndexes = indexes.get(tableName);
            tableIndexes.put(indexName, indexMap);
        });
    }

    public CompletableFuture<Void> insertTuple(Tuple tuple) {
        return CompletableFuture.runAsync(() -> {
            String tableName = tuple.id().tableName();
            TableMetadata metadata = tables.get(tableName);
            List<Class<?>> columnTypes = metadata.columns().stream()
                .map(column -> column.type().getJavaType())
                .collect(Collectors.toList());

            // Store tuple
            tuples.get(tableName).put(tuple.id(), tuple);

            System.out.println("INSERTED: " + tuple.getValues(columnTypes) + ", Total: " + tuples.get(tableName).size());
            // Update indexes
            Map<String, Map<String, Set<TupleId>>> tableIndexes = indexes.get(tableName);
            for (Map.Entry<String, Map<String, Set<TupleId>>> indexEntry : tableIndexes.entrySet()) {
                String indexName = indexEntry.getKey();
                IndexMetadata indexMetadata = metadata.indexes().get(indexName);
                if (indexMetadata == null) continue;

                List<Object> values = tuple.getValues(columnTypes);
                String indexKey = buildIndexKey(tableName, indexMetadata.columnNames(), values);
                indexEntry.getValue().computeIfAbsent(indexKey, k -> new HashSet<>()).add(tuple.id());
            }
        });
    }

    public CompletableFuture<List<Tuple>> findTuples(String tableName, Map<String, Object> conditions) {
        return CompletableFuture.supplyAsync(() -> {
            TableMetadata metadata = tables.get(tableName);
            List<Class<?>> columnTypes = metadata.columns().stream()
                .map(column -> column.type().getJavaType())
                .collect(Collectors.toList());

            System.out.println("FINDING: " + metadata + ", " + tuples.get(tableName).entrySet() + ", Conditions: " + conditions);
            // Find matching tuples
            return tuples.get(tableName).entrySet().stream()
                .filter(entry -> matchesConditions(entry.getValue(), conditions, columnTypes))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        });
    }

    private boolean matchesConditions(Tuple tuple, Map<String, Object> conditions, List<Class<?>> types) {
        if (conditions == null || conditions.isEmpty()) {
            return true;
        }

        List<Object> values = tuple.getValues(types);
        List<String> columnNames = tables.get(tuple.id().tableName()).columns().stream()
            .map(column -> column.name())
            .collect(Collectors.toList());

        System.out.println("MATCHING: " + tuple + ", " + columnNames + ", " + values);

        return conditions.entrySet().stream().allMatch(entry -> {
            int columnIndex = columnNames.indexOf(entry.getKey());
            if (columnIndex == -1) {
                return false;
            }
            Object value = values.get(columnIndex);
            return entry.getValue().equals(value);
        });
    }

    private String buildIndexKey(String tableName, List<String> indexColumns, List<Object> values) {
        List<String> columnNames = tables.get(tableName).columns().stream()
            .map(column -> column.name())
            .collect(Collectors.toList());

        return indexColumns.stream()
            .map(columnName -> {
                int columnIndex = columnNames.indexOf(columnName);
                return values.get(columnIndex).toString();
            })
            .collect(Collectors.joining(":"));
    }

    public CompletableFuture<TableMetadata> getTableMetadata(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        return CompletableFuture.completedFuture(tables.get(tableName));
    }

    public CompletableFuture<Transaction> beginTransaction() {
        // TODO: Implement transaction support
        return CompletableFuture.completedFuture(null);
    }
} 