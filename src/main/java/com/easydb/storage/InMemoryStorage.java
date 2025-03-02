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
    public List<Tuple> scanTuples(String tableName, Map<String, Object> conditions, Transaction txn) {
        // First get all base tuple IDs for the table
        List<TupleId> tableTupleIds = tableTuples.keySet().stream()
            .filter(id -> id.tableName().equals(tableName))
            .map(TupleId::getBaseId)  // Get base version IDs
            .distinct()  // Remove duplicates
            .collect(Collectors.toList());
        
        // For each base tuple ID, get the visible version using getTuple
        List<Tuple> tuples = tableTupleIds.stream()
            .map(tupleId -> getTuple(tupleId, txn))  // Use getTuple for MVCC visibility
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(tuple -> matchesConditions(tuple, conditions))
            .collect(Collectors.toList());
        System.out.println("scanTuples - Found " + tuples.size() + " tuples");
        System.out.println("scanTuples - Conditions: " + conditions);
        System.out.println("scanTuples - Trasnaction Id: " + txn.getXid());
        return tuples;
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

        System.out.println("updateTuple - Updating tuple: " + newVersion);
        System.out.println("updateTuple - Current tuple: " + currentTuple);
        System.out.println("updateTuple - Transaction: " + txn.getXid());
       
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
        
        System.out.println("getTuple - Looking for tupleId: " + tupleId);
        System.out.println("  Current transaction: " + txn.getXid());
        
        if (tuple == null) {
            System.out.println("  Base tuple not found");
            return Optional.empty();
        }

        // Follow version chain until we find a visible version
        Tuple currentVersion = tuple;
        Tuple visibleVersion = null;

        while (currentVersion != null) {
            System.out.println("    currentVersion: " + currentVersion);
            
            if (isVisible(currentVersion, txn)) {
                visibleVersion = currentVersion;
            }
            
            // Move to next version
            TupleId nextId = currentVersion.getNextVersionId();
            if (nextId == null || nextId.equals(currentVersion.id())) {
                System.out.println("  End of version chain");
                break;
            }
            currentVersion = tableTuples.get(nextId);
        }

        if (visibleVersion != null) {
            System.out.println("Found visible version: " + visibleVersion.id());
            txn.recordRead(visibleVersion.id());
        } else {
            System.out.println("No visible version found");
        }

        return Optional.ofNullable(visibleVersion);
    }

    private boolean isVisible(Tuple tuple, Transaction txn) {
        long xmin = tuple.getXmin();  // Creating transaction
        long xmax = tuple.getXmax();  // Deleting transaction

        // Check if creating transaction is visible
        if (!transactionManager.isCommitted(xmin) && xmin != txn.getXid()) {
            System.out.println("    Not visible: creator not committed and not current transaction");
            return false;
        }

        // Check if tuple is deleted
        if (xmax != 0) {
            if (xmax == txn.getXid()) {
                System.out.println("    Not visible: deleted by current transaction");
                return false;
            }
            if (transactionManager.isCommitted(xmax)) {
                System.out.println("    Not visible: deleted by committed transaction");
                return false;
            }
        }

        System.out.println("    Visible: all checks passed");
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

