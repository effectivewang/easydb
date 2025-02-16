package com.easydb.storage;

import com.easydb.storage.metadata.TableMetadata;
import java.util.*;

/**
 * Header information for a tuple, similar to PostgreSQL's HeapTupleHeaderData.
 */
public class TupleHeader {
    private final TupleId id;
    private final TableMetadata metadata;
    private final List<Integer> valueLengths;
    private final Map<String, Integer> columnPositions;
    
    // MVCC information
    private final long xmin;           // Creating transaction ID
    private final long xmax;           // Deleting transaction ID
    private final boolean updated;     // Has this tuple been updated?
    private final byte[] nullBitmap;   // Bitmap for NULL values

    public TupleHeader(
            TupleId id,
            TableMetadata metadata,
            List<Integer> valueLengths,
            long xmin,
            long xmax,
            boolean updated,
            byte[] nullBitmap) {
        this.id = id;
        this.metadata = metadata;
        this.valueLengths = valueLengths;
        this.xmin = xmin;
        this.xmax = xmax;
        this.updated = updated;
        this.nullBitmap = nullBitmap;

        // Build column position mapping
        this.columnPositions = new HashMap<>();
        List<String> columnNames = metadata.columnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            columnPositions.put(columnNames.get(i), i);
        }
    }

    public TupleHeader withXmax(long xmax) {
        return new TupleHeader(
            id, metadata, valueLengths, xmin, xmax, updated, nullBitmap);
    }

    public TupleHeader withUpdate(long xmax) {
        return new TupleHeader(
            id, metadata, valueLengths, xmin, xmax, true, nullBitmap);
    }

    public boolean isVisible(long currentXid) {
        // Tuple is visible if:
        // 1. Created by committed transaction (xmin < currentXid)
        // 2. Not deleted (xmax == 0) OR deleted by uncommitted transaction (xmax > currentXid)
        return xmin < currentXid && (xmax == 0 || xmax > currentXid);
    }

    public boolean isDeleted() {
        return xmax != 0;
    }

    public boolean isNull(int columnIndex) {
        int byteIndex = columnIndex / 8;
        int bitIndex = columnIndex % 8;
        return (nullBitmap[byteIndex] & (1 << bitIndex)) != 0;
    }

    public int getColumnPosition(String columnName) {
        Integer position = columnPositions.get(columnName);
        if (position == null) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        return position;
    }

    // Getters
    public TupleId getId() { return id; }
    public TableMetadata getMetadata() { return metadata; }
    public List<Integer> getValueLengths() { return valueLengths; }
    public long getXmin() { return xmin; }
    public long getXmax() { return xmax; }
    public boolean isUpdated() { return updated; }
} 