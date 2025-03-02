package com.easydb.storage;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.transaction.Transaction;
import com.easydb.storage.transaction.TransactionManager;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Represents a tuple (row) in a table.
 * Similar to PostgreSQL's HeapTuple structure.
 */
public class Tuple {
    private final TupleId id;
    private final List<Object> values;
    private final TupleHeader header;
    private volatile TupleId nextVersionId;  // Points to next version (PostgreSQL's t_ctid)

    public Tuple(TupleId id, List<Object> values, TupleHeader header, long xmin, long xmax) {
        this.id = id;
        this.values = new ArrayList<>(values);
        this.header = header.withXmin(xmin).withXmax(xmax);
        this.nextVersionId = id;  // Initially points to self (like PostgreSQL)
    }

    public Tuple(TupleId id, List<Object> values, TupleHeader header, long xmin) {
        this(id, values, header, xmin, 0L);
    }

    // Version chain management
    public void setNextVersion(TupleId nextId) {
        this.nextVersionId = nextId;
    }

    public TupleId getNextVersionId() {
        return nextVersionId;
    }

    // MVCC support
    public boolean isVisible(Transaction txn) {
        switch (txn.getIsolationLevel()) {
            case READ_COMMITTED:
                return isVisibleForReadCommitted(txn);
            case REPEATABLE_READ:
            case SERIALIZABLE:
                return isVisibleForSnapshot(txn);
            default:
                throw new IllegalStateException("Unknown isolation level");
        }
    }

    private boolean isVisibleForReadCommitted(Transaction txn) {
        // Creator must be committed
        if (!txn.isCommitted(header.getXmin()) && header.getXmin() != txn.getXid()) {
            return false;
        }

        // Not deleted or deleter not committed
        return header.getXmax() == 0 || 
               !txn.isCommitted(header.getXmax()) || 
               header.getXmax() == txn.getXid();
    }

    private boolean isVisibleForSnapshot(Transaction txn) {
        // Must be created before snapshot
        if (header.getXmin() >= txn.getXid() || 
            txn.wasActiveAtSnapshot(header.getXmin())) {
            return false;
        }

        // Not deleted or deleted after snapshot
        return header.getXmax() == 0 || 
               header.getXmax() >= txn.getXid() || 
               txn.wasActiveAtSnapshot(header.getXmax());
    }

    // Getters and setters
    public long getXmin() { return header.getXmin(); }
    public long getXmax() { return header.getXmax(); }
    
    public TupleId id() {
        return id;
    }

    public <T> T getValue(String columnName, Class<T> expectedType) {
        int position = header.getColumnPosition(columnName);
        Object value = values.get(position);
        
        if (value == null || expectedType.isInstance(value)) {
            return expectedType.cast(value);
        }
        throw new ClassCastException(
            "Column '" + columnName + "' is not of type " + expectedType.getSimpleName());
    }

    public Object getValue(String columnName) {
        return values.get(header.getColumnPosition(columnName));
    }

    public Object getValue(int index) {
        return values.get(index);
    }

    public List<String> getColumnNames() {
        return header.getMetadata().columnNames();
    }

    public List<Object> getValues() {
        return new ArrayList<>(values);
    }

    public TupleHeader getHeader() {
        return header;
    }

    public Tuple markDeleted(long xmax) {
        TupleHeader newHeader = header.withXmax(xmax);
        return new Tuple(id, values, newHeader, header.getXmin(), xmax);
    }

    public Tuple withUpdatedValues(Map<String, Object> updates, long xmax) {
        List<Object> newValues = new ArrayList<>(values);
        
        for (Map.Entry<String, Object> update : updates.entrySet()) {
            int position = header.getColumnPosition(update.getKey());
            newValues.set(position, update.getValue());
        }

        // Create new header with updated MVCC info
        TupleHeader newHeader = header.withUpdate(xmax);
        
        // Recalculate lengths and serialize
        List<Integer> newLengths = newValues.stream()
            .map(ByteUtils::getSerializedLength)
            .collect(Collectors.toList());
            
        byte[] newValueBytes = ByteUtils.serializeValues(newValues, newLengths);

        return new Tuple(id, newValues, newHeader, header.getXmin(), xmax);
    }
    public boolean isDeleted() {
        return header.getXmax() != 0;
    }

    public TableMetadata getMetadata() {
        return header.getMetadata();
    }
    public String toString() {
        return "Tuple{" +
            "id=" + id +
            ", values=" + Arrays.toString(values.toArray()) +
            ", header=" + header +
            '}';
    }
} 
