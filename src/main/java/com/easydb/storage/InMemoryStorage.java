package com.easydb.storage;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.index.HashIndex;
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
    private final Map<String, HashIndex<TupleId, Tuple>> tupleIndexes;
    private final Map<String, Map<String, Map<String, Set<TupleId>>>> indexes;

    public InMemoryStorage() {
        this.tables = new ConcurrentHashMap<>();
        this.tupleIndexes = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<Void> createTable(TableMetadata metadata) {
        return CompletableFuture.runAsync(() -> {
            tables.put(metadata.tableName(), metadata);
            tupleIndexes.put(metadata.tableName(), new HashIndex<>(1000));
            indexes.put(metadata.tableName(), new ConcurrentHashMap<>());
        });
    }

    @Override
    public CompletableFuture<Void> createIndex(IndexMetadata metadata) {
        return CompletableFuture.runAsync(() -> {
            String tableName = metadata.tableName();
            String indexName = metadata.indexName();
            TableMetadata tableMetadata = tables.get(tableName);
            
            // Create index map
            Map<String, Set<TupleId>> indexMap = new ConcurrentHashMap<>();
            
            // Index existing tuples
            Map<TupleId, Tuple> tableTuples = tupleIndexes.get(tableName).range(TupleId.MIN, TupleId.MAX)
                .join()
                .stream()
                .collect(Collectors.toMap(
                    tuple -> tuple.id(),
                    tuple -> tuple
                ));
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

    @Override
    public CompletableFuture<Void> insertTuple(Tuple tuple) {
        return CompletableFuture.runAsync(() -> {
            String tableName = tuple.id().tableName();
            HashIndex<TupleId, Tuple> tupleIndex = tupleIndexes.get(tableName);
            
            // Insert into main index
            tupleIndex.insert(tuple.id(), tuple);

            // Update secondary indexes
            TableMetadata metadata = tables.get(tableName);
            List<Class<?>> columnTypes = metadata.columns().stream()
                .map(column -> column.type().getJavaType())
                .collect(Collectors.toList());

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

    @Override
    public CompletableFuture<List<Tuple>> findTuples(String tableName, Map<String, Object> conditions) {
        return CompletableFuture.supplyAsync(() -> {
            TableMetadata metadata = tables.get(tableName);
            List<Class<?>> columnTypes = metadata.columns().stream()
                .map(column -> column.type().getJavaType())
                .collect(Collectors.toList());

            HashIndex<TupleId, Tuple> tupleIndex = tupleIndexes.get(tableName);
            
            // Use index for lookup if possible
            if (conditions != null && !conditions.isEmpty() && metadata.indexes() != null) {
                // Find best matching index
                for (IndexMetadata idx : metadata.indexes().values()) {
                    if (idx.columnNames().stream().allMatch(conditions::containsKey)) {
                        String indexKey = buildIndexKey(tableName, idx.columnNames(), 
                            idx.columnNames().stream()
                                .map(conditions::get)
                                .collect(Collectors.toList()));
                        
                        Set<TupleId> tupleIds = indexes.get(tableName)
                            .get(idx.indexName())
                            .get(indexKey);
                            
                        if (tupleIds != null) {
                            return tupleIds.stream()
                                .map(id -> tupleIndex.search(id).join())
                                .filter(tuple -> tuple != null && matchesConditions(tuple, conditions, columnTypes))
                                .collect(Collectors.toList());
                        }
                    }
                }
            }

            // Fall back to full scan if no index matches
            return tupleIndex.range(TupleId.MIN, TupleId.MAX).join().stream()
                .filter(tuple -> matchesConditions(tuple, conditions, columnTypes))
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