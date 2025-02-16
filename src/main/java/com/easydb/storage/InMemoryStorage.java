package com.easydb.storage;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.index.HashIndex;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicLong;
import com.easydb.storage.transaction.*;

/**
 * In-memory implementation of the storage engine.
 */
public class InMemoryStorage implements Storage {
    private final Map<String, TableMetadata> tables;
    private final Map<TupleId, Tuple> tableTuples;
    private final Map<String, HashIndex<String, TupleId>> indexMap;

    public InMemoryStorage() {
        this.tables = new ConcurrentHashMap<>();
        this.tableTuples = new ConcurrentHashMap<>();
        this.indexMap = new ConcurrentHashMap<>();
    }

    @Override
    public void createTable(TableMetadata metadata) {
        tables.put(metadata.tableName(), metadata);
    }

    @Override
    public void createIndex(IndexMetadata metadata) {
            TableMetadata tableMetadata = tables.get(metadata.tableName());
            if (tableMetadata == null) {
                throw new IllegalArgumentException("Table not found: " + metadata.tableName());
            }

            String indexName = metadata.indexName();
            indexMap.put(indexName, new HashIndex<>(1000));

            tableMetadata.indexes().put(indexName, metadata);
            
            // Index existing tuples
            tableTuples.values().stream()
                .filter(tuple -> tuple.id().tableName().equals(metadata.tableName()))
                .forEach(tuple -> {
                    String indexKey = buildIndexKey(metadata, tuple);
                indexMap.get(indexName).insert(indexKey, tuple.id()).join();
            });
            
    }

    @Override
    public void insertTuple(Tuple tuple) {
        String tableName = tuple.id().tableName();
        TableMetadata metadata = tables.get(tableName);
        
        // Store in primary storage
        tableTuples.put(tuple.id(), tuple);

        // Update indexes
        updateIndexes(metadata, tuple);
    }

    @Override
    public TableMetadata getTableMetadata(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        return tables.get(tableName);
    }

    @Override
    public List<Tuple> scanTuples(String tableName, Map<String, Object> conditions) {
        TableMetadata metadata = tables.get(tableName);
        List<Class<?>> columnTypes = metadata.columnTypes();

        System.out.println("Scanning table: " + tableName);
        System.out.println("Conditions: " + conditions);
        // Fall back to full scan
        List<Tuple> tuples = tableTuples.values().stream()
                .filter(tuple -> tuple.id().tableName().equals(tableName))
                .filter(tuple -> matchesConditions(tuple, conditions, columnTypes))
                .collect(Collectors.toList());

        System.out.println("Tuples: " + tuples);
        return tuples;
    }

    @Override
    public List<Tuple> getTuples(List<TupleId> tupleIds) {
        return tupleIds.stream()
            .map(tableTuples::get)
            .collect(Collectors.toList());
    }

    private boolean matchesConditions(Tuple tuple, Map<String, Object> conditions, List<Class<?>> types) {
        if (conditions == null || conditions.isEmpty()) {
            return true;
        }

        List<Object> values = tuple.getValues();
        List<String> columnNames = tables.get(tuple.id().tableName()).columns().stream()
            .map(column -> column.name())
            .collect(Collectors.toList());

        boolean matches = conditions.entrySet().stream().allMatch(entry -> {
            int columnIndex = columnNames.indexOf(entry.getKey());
            if (columnIndex == -1) {
                return false;
            }
            Object value = values.get(columnIndex);
            System.out.println("Value: " + value + "type: " + value.getClass());
            System.out.println("Entry value: " + entry.getValue() + "type: " + entry.getValue().getClass());
            boolean match = entry.getValue().equals(value);
            System.out.println("Match: " + match);
            return match;
        });

        System.out.println("Matches: " + matches);
        return matches;
    }

    private String buildIndexKey(IndexMetadata indexMetadata, Tuple tuple) {
        List<String> indexedColumns = indexMetadata.columnNames();
        TableMetadata metadata = tables.get(tuple.id().tableName());
        List<Object> values = tuple.getValues();
        List<Object> indexValues = new ArrayList<>();
        
        // Only use values from indexed columns
        for (String columnName : indexedColumns) {
            int columnIndex = metadata.columnNames().indexOf(columnName);
            if (columnIndex >= 0) {
                indexValues.add(values.get(columnIndex));
            }
        }
        
        return buildTypedKey(indexValues);
    }

    private String buildTypedKey(List<Object> values) {
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (i > 0) key.append(":");
            
            if (value instanceof Number) {
                // Pad numbers for proper lexicographical ordering
                key.append(String.format("%020d", ((Number) value).longValue()));
            } else if (value instanceof String) {
                // Escape special characters
                key.append(((String) value).replace(":", "\\:"));
            } else {
                key.append(value);
            }
        }
        return key.toString();
    }

    private void updateIndexes(TableMetadata metadata, Tuple tuple) {
        if (metadata.indexes() != null) {
            for (Map.Entry<String, IndexMetadata> indexEntry : metadata.indexes().entrySet()) {
                String indexName = indexEntry.getKey();
                IndexMetadata indexMetadata = indexEntry.getValue();
                HashIndex<String, TupleId> index = indexMap.get(indexName);
                
                if (index != null) {
                    String indexKey = buildIndexKey(indexMetadata, tuple);
                    Transaction txn = null;
                    index.insert(indexKey, tuple.id()).join();
                }
            }
        }
    }
}

