package com.easydb.storage;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.index.HashIndex;
import com.easydb.core.Transaction;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicLong;
import com.easydb.core.TupleId;
import com.easydb.core.Tuple;
import com.easydb.core.IsolationLevel;

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
    public CompletableFuture<Void> createTable(TableMetadata metadata) {
        return CompletableFuture.runAsync(() -> {
            tables.put(metadata.tableName(), metadata);
        });
    }

    @Override
    public CompletableFuture<Void> createIndex(IndexMetadata metadata) {
        return CompletableFuture.runAsync(() -> {
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
                    Transaction txn = null;
                    indexMap.get(indexName).insert(indexKey, tuple.id(), txn).join();
                });
            
        });
    }

    @Override
    public CompletableFuture<Void> insertTuple(Tuple tuple) {
        return CompletableFuture.runAsync(() -> {
            String tableName = tuple.id().tableName();
            TableMetadata metadata = tables.get(tableName);
            
            // Store in primary storage
            tableTuples.put(tuple.id(), tuple);

            // Update indexes
            updateIndexes(metadata, tuple);
        });
    }

    @Override
    public CompletableFuture<Void> insertTuple(Tuple tuple, Transaction txn) {
        return CompletableFuture.runAsync(() -> {
        });
    }

    @Override
    public CompletableFuture<Void> applyTransactionChanges(Tuple tuple, Transaction txn) {
        return CompletableFuture.runAsync(() -> {
            // 1. Update base storage
            tableTuples.put(tuple.id(), tuple);

        // 2. Update indexes
        TableMetadata metadata = tables.get(tuple.id().tableName());
        if (metadata.indexes() != null) {
            for (Map.Entry<String, IndexMetadata> indexEntry : metadata.indexes().entrySet()) {
                String indexName = indexEntry.getKey();
                IndexMetadata indexMetadata = indexEntry.getValue();
                HashIndex<String, TupleId> index = indexMap.get(indexName);
                
                if (index != null) {
                    String indexKey = buildIndexKey(indexMetadata, tuple);
                    
                    // Remove old index entry if exists
                    Tuple oldTuple = tableTuples.get(tuple.id());
                    if (oldTuple != null) {
                        String oldIndexKey = buildIndexKey(indexMetadata, oldTuple);
                        index.delete(oldIndexKey, txn).join();
                    }

                        // Insert new index entry
                        index.insert(indexKey, tuple.id(), txn).join();
                }
            }
        }});
    }   

    public CompletableFuture<List<Tuple>> findTuples(String tableName, Map<String, Object> conditions) {
        return CompletableFuture.supplyAsync(() -> {
            TableMetadata metadata = tables.get(tableName);
            List<Class<?>> columnTypes = metadata.columnTypes();

            // Try using index if conditions match
            if (conditions != null && !conditions.isEmpty() && metadata.indexes() != null) {
                for (Map.Entry<String, IndexMetadata> indexEntry : metadata.indexes().entrySet()) {
                    String indexName = indexEntry.getKey();
                    IndexMetadata indexMetadata = indexEntry.getValue();
                    List<String> indexColumns = indexMetadata.columnNames();
                    
                    // Check if all indexed columns have conditions
                    if (indexColumns.stream().allMatch(conditions::containsKey)) {
                        List<Object> indexValues = indexColumns.stream()
                            .map(conditions::get)
                            .collect(Collectors.toList());
                            
                        String indexKey = buildTypedKey(indexValues);
                        HashIndex<String, TupleId> index = indexMap.get(indexName);
                        
                        if (index != null) {
                            CompletableFuture<TupleId> tupleIdFuture = index.search(indexKey, null);
                            if (tupleIdFuture != null) {
                                TupleId tupleId = tupleIdFuture.join();
                                if (tupleId != null) {
                                    Tuple tuple = tableTuples.get(tupleId);
                                    if (tuple != null && matchesConditions(tuple, conditions, columnTypes)) {
                                        return Collections.singletonList(tuple);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Fall back to full scan
            return tableTuples.values().stream()
                .filter(tuple -> tuple.id().tableName().equals(tableName))
                .filter(tuple -> matchesConditions(tuple, conditions, columnTypes))
                .collect(Collectors.toList());
        });
    }

    public CompletableFuture<List<Tuple>> findTuples(String tableName, Map<String, Object> conditions, Transaction txn) {
        return CompletableFuture.supplyAsync(() -> {
            // TODO: Implement findTuples for transactions
            return Collections.emptyList();
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

        return conditions.entrySet().stream().allMatch(entry -> {
            int columnIndex = columnNames.indexOf(entry.getKey());
            if (columnIndex == -1) {
                return false;
            }
            Object value = values.get(columnIndex);
            return entry.getValue().equals(value);
        });
    }

    private String buildIndexKey(IndexMetadata indexMetadata, Tuple tuple) {
        List<String> indexedColumns = indexMetadata.columnNames();
        TableMetadata metadata = tables.get(tuple.id().tableName());
        List<Object> values = tuple.getValues(metadata.columnTypes());
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
                    index.insert(indexKey, tuple.id(), txn).join();
                }
            }
        }
    }

    public CompletableFuture<TableMetadata> getTableMetadata(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        return CompletableFuture.completedFuture(tables.get(tableName));
    }
     
}

