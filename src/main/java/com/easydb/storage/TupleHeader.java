package com.easydb.storage;

import com.easydb.storage.metadata.TableMetadata;
import java.util.*;

/**
 * Header information for a tuple, similar to PostgreSQL's HeapTupleHeaderData.
 */
public class TupleHeader {
    private final TupleId id;
    private final TableMetadata metadata;
    private final Map<String, Integer> columnPositions;
    
    // MVCC information
    private final long xmin;           // Creating transaction ID
    private final long xmax;           // Deleting transaction ID

    public TupleHeader(
            TupleId id,
            TableMetadata metadata,
            long xmin,
            long xmax) {
        this.id = id;
        this.metadata = metadata;
        this.xmin = xmin;
        this.xmax = xmax;
        // Build column position mapping
        this.columnPositions = new HashMap<>();
        List<String> columnNames = metadata.columnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            columnPositions.put(columnNames.get(i), i);
        }
    }

    public TupleHeader withXmin(long xmin) {
        return new TupleHeader(
            id, metadata, xmin, xmax);
    }

    public TupleHeader withXmax(long xmax) {
        return new TupleHeader(
            id, metadata, xmin, xmax);
    }

    public TupleHeader withUpdate(long xmax) {
        return new TupleHeader(
            id, metadata, xmin, xmax);
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
    public long getXmin() { return xmin; }
    public long getXmax() { return xmax; }

    @Override
    public String toString() {
        return "TupleHeader{" +
            "id=" + id +
            ", metadata=" + metadata +
            ", xmin=" + xmin +
            ", xmax=" + xmax +
            '}';
    }

} 