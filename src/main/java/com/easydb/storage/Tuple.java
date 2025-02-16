package com.easydb.storage;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import com.easydb.storage.metadata.TableMetadata;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Represents a tuple (row) in a table.
 * Similar to PostgreSQL's HeapTuple structure.
 */
public class Tuple {
    private final TupleHeader header;
    private final byte[] values;
    private final List<Object> rowValues;

    public Tuple(TupleId id, List<Object> rowValues, TableMetadata metadata, long xmin) {
        this.rowValues = new ArrayList<>(rowValues);
        
        // Calculate value lengths for variable-length fields
        List<Integer> valueLengths = rowValues.stream()
            .map(ByteUtils::getSerializedLength)
            .collect(Collectors.toList());

        // Create header with MVCC info
        this.header = new TupleHeader(
            id,
            metadata,
            valueLengths,
            xmin,          // Creating transaction ID
            0L,            // Not deleted yet
            false,         // Not updated yet
            createNullBitmap(rowValues)
        );

        // Serialize values
        this.values = ByteUtils.serializeValues(rowValues, valueLengths);
    }

    // Private constructor for updates
    private Tuple(TupleHeader header, List<Object> rowValues, byte[] values) {
        this.header = header;
        this.rowValues = rowValues;
        this.values = values;
    }

    public long getXmin() {
        return header.getXmin();
    }

    public TupleId id() {
        return header.getId();
    }

    public <T> T getValue(String columnName, Class<T> expectedType) {
        int position = header.getColumnPosition(columnName);
        Object value = rowValues.get(position);
        
        if (value == null || expectedType.isInstance(value)) {
            return expectedType.cast(value);
        }
        throw new ClassCastException(
            "Column '" + columnName + "' is not of type " + expectedType.getSimpleName());
    }

    public Object getValue(String columnName) {
        return rowValues.get(header.getColumnPosition(columnName));
    }

    public Object getValue(int index) {
        return rowValues.get(index);
    }

    public List<String> getColumnNames() {
        return header.getMetadata().columnNames();
    }

    public List<Object> getValues() {
        return new ArrayList<>(rowValues);
    }

    public Tuple markDeleted(long xmax) {
        TupleHeader newHeader = header.withXmax(xmax);
        return new Tuple(newHeader, rowValues, values);
    }

    public Tuple withUpdatedValues(Map<String, Object> updates, long xmax) {
        List<Object> newValues = new ArrayList<>(rowValues);
        
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

        return new Tuple(newHeader, newValues, newValueBytes);
    }

    private static byte[] createNullBitmap(List<Object> values) {
        int numBytes = (values.size() + 7) / 8;  // Round up to nearest byte
        byte[] bitmap = new byte[numBytes];
        
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) == null) {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                bitmap[byteIndex] |= (1 << bitIndex);
            }
        }
        
        return bitmap;
    }

    public boolean isVisible(long currentXid) {
        return header.isVisible(currentXid);
    }

    public boolean isDeleted() {
        return header.isDeleted();
    }

    public TableMetadata getMetadata() {
        return header.getMetadata();
    }
} 
