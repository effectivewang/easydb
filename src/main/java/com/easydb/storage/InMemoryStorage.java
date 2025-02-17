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
import java.util.Optional;


/**
 * In-memory implementation of the storage engine.
 */
public class InMemoryStorage implements Storage {
    private final Map<String, TableMetadata> tables;
    private final Map<TupleId, Tuple> tableTuples;
    private final Map<String, HashIndex<String, TupleId>> indexMap;
    private final TransactionManager transactionManager;

    public InMemoryStorage(TransactionManager transactionManager) {
        this.tables = new ConcurrentHashMap<>();
        this.tableTuples = new ConcurrentHashMap<>();
        this.indexMap = new ConcurrentHashMap<>();
        this.transactionManager = transactionManager;
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
    public void insertTuple(Tuple tuple, Transaction txn) {
        String tableName = tuple.id().tableName();
        TableMetadata metadata = tables.get(tableName);
        if (metadata == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        // Create initial version (v0) of the tuple
        TupleId v0Id = tuple.id().withVersion(0);  // Ensure we're using version 0
        Tuple v0Tuple = new Tuple(
            v0Id,
            tuple.getValues(),
            tuple.getHeader(),
            txn.getXid(),     // xmin (creating transaction)
            0L               // xmax (not deleted)
        );

        // Record write in transaction
        txn.recordWrite(v0Id);
        
        // Store in primary storage
        tableTuples.put(v0Id, v0Tuple);

        // Update indexes
        updateIndexes(metadata, v0Tuple, txn);
    }

    @Override
    public TableMetadata getTableMetadata(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        return tables.get(tableName);
    }

    @Override
    public List<Tuple> scanTuples(String tableName, Map<String, Object> conditions, 
                                 Transaction txn) {
        return tableTuples.values().stream()
            .filter(tuple -> tuple.id().tableName().equals(tableName))
            // Use Tuple's isVisible method
            .filter(tuple -> tuple.isVisible(txn))
            .filter(tuple -> matchesConditions(tuple, conditions))
            .peek(tuple -> txn.recordRead(tuple.id()))
            .collect(Collectors.toList());
    }

    @Override
    public List<Tuple> getTuples(List<TupleId> tupleIds, Transaction txn) {
        return tupleIds.stream()
            .map(tupleId -> getTuple(tupleId, txn))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

    @Override
    public void updateTuple(TupleId tupleId, List<Object> newValues, Transaction txn) {
        Tuple currentTuple = tableTuples.getOrDefault(tupleId, null);
        if (currentTuple == null || !currentTuple.isVisible(txn)) {
            throw new IllegalStateException("Tuple not visible to transaction");
        }

        // Create new version
        TupleId newVersionId = tupleId.nextVersion();
        Tuple newVersion = new Tuple(
            newVersionId,
            newValues,
            currentTuple.getHeader(),
            txn.getXid(),  // xmin (creating transaction)
            0L            // xmax (not deleted)
        );

        // Update version chain
        currentTuple = new Tuple(
            currentTuple.id(),
            currentTuple.getValues(),
            currentTuple.getHeader(),
            currentTuple.getXmin(),
            txn.getXid()
        );
        currentTuple.setNextVersion(newVersionId);  // Point to new version

        // Store new version
        tableTuples.put(newVersionId, newVersion);
        txn.recordWrite(newVersionId);

        // Update indexes with new version
        TableMetadata metadata = tables.get(newVersionId.tableName());
        updateIndexes(metadata, newVersion, txn);
    }

    @Override
    public void deleteTuple(TupleId tupleId, Transaction txn) {
        Tuple currentTuple = tableTuples.get(tupleId);
        if (currentTuple == null || !currentTuple.isVisible(txn)) {
            throw new IllegalStateException("Tuple not visible to transaction");
        }

        // Mark tuple as deleted by setting xmax
        currentTuple = new Tuple(
            currentTuple.id(),
            currentTuple.getValues(),
            currentTuple.getHeader(),
            currentTuple.getXmin(),
            txn.getXid()
        );
        txn.recordWrite(tupleId);

        // Remove from indexes
        TableMetadata metadata = tables.get(tupleId.tableName());
        removeFromIndexes(metadata, currentTuple);
    }

    @Override
    public Optional<Tuple> getTuple(TupleId tupleId, Transaction txn) {
        // Get base version (v0)
        TupleId baseId = tupleId.getBaseId();
        Tuple tuple = tableTuples.get(baseId);
        
        if (tuple == null) {
            return Optional.empty();
        }

        // Follow version chain until we find a visible version
        Tuple currentVersion = tuple;
        Tuple visibleVersion = null;

        while (currentVersion != null) {
            if (isVisible(currentVersion, txn)) {
                visibleVersion = currentVersion;
                break;  // Found the visible version
            }
            
            // Move to next version
            TupleId nextId = currentVersion.getNextVersionId();
            if (nextId == null || nextId.equals(currentVersion.id())) {
                break;  // End of chain
            }
            currentVersion = tableTuples.get(nextId);
        }

        if (visibleVersion != null) {
            txn.recordRead(visibleVersion.id());
        }

        return Optional.ofNullable(visibleVersion);
    }

    private boolean isVisible(Tuple tuple, Transaction txn) {
        long xmin = tuple.getXmin();  // Creating transaction
        long xmax = tuple.getXmax();  // Deleting transaction (or 0 if not deleted)

        // Check if creating transaction is visible
        if (!transactionManager.isCommitted(xmin) && xmin != txn.getXid()) {
            return false;  // Created by uncommitted transaction (except our own)
        }

        // Check if tuple is deleted
        if (xmax != 0) {
            if (xmax == txn.getXid()) {
                return false;  // Deleted by current transaction
            }
            if (transactionManager.isCommitted(xmax)) {
                return false;  // Deleted by committed transaction
            }
        }

        return true;
    }

    private boolean matchesConditions(Tuple tuple, Map<String, Object> conditions) {
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

    private void updateIndexes(TableMetadata metadata, Tuple tuple, Transaction txn) {
        if (metadata.indexes() != null) {
            for (Map.Entry<String, IndexMetadata> indexEntry : metadata.indexes().entrySet()) {
                String indexName = indexEntry.getKey();
                IndexMetadata indexMetadata = indexEntry.getValue();
                HashIndex<String, TupleId> index = indexMap.get(indexName);
                
                if (index != null) {
                    String indexKey = buildIndexKey(indexMetadata, tuple);
                    index.insert(indexKey, tuple.id()).join();
                }
            }
        }
    }

    private void removeFromIndexes(TableMetadata metadata, Tuple tuple) {
        if (metadata.indexes() != null) {
            for (Map.Entry<String, IndexMetadata> indexEntry : metadata.indexes().entrySet()) {
                String indexName = indexEntry.getKey();
                IndexMetadata indexMetadata = indexEntry.getValue();
                HashIndex<String, TupleId> index = indexMap.get(indexName);
                
                if (index != null) {
                    String indexKey = buildIndexKey(indexMetadata, tuple);
                    index.delete(indexKey).join();
                }
            }
        }
    }
}

