package com.easydb.storage;

import java.time.Instant;
import java.util.UUID;
import java.util.Objects;

/**
 * Represents a unique identifier for a tuple.
 */
public class TupleId implements Comparable<TupleId> {
    private final String tableName;
    private final long rowId;      // Physical row identifier
    private final long version;    // Version number

    public static final TupleId MIN = new TupleId("", Long.MIN_VALUE, 0);
    public static final TupleId MAX = new TupleId("", Long.MAX_VALUE, 0);

    public TupleId(String tableName, long rowId, long version) {
        this.tableName = tableName;
        this.rowId = rowId;
        this.version = version;
    }

    // Create initial version
    public TupleId(String tableName, long rowId) {
        this(tableName, rowId, 0);
    }

    // Create next version
    public TupleId nextVersion() {
        return new TupleId(tableName, rowId, version + 1);
    }

    // Get base TupleId (version 0)
    public TupleId getBaseId() {
        return new TupleId(tableName, rowId, 0);
    }

    public String tableName() {
        return tableName;
    }

    public long rowId() {
        return rowId;
    }

    public long version() {
        return version;
    }

    public TupleId withVersion(long version) {
        return new TupleId(tableName, rowId, version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TupleId)) return false;
        TupleId that = (TupleId) o;
        return rowId == that.rowId && 
               version == that.version && 
               Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, rowId, version);
    }

    @Override
    public String toString() {
        return String.format("%s:%d:v%d", tableName, rowId, version);
    }

    @Override
    public int compareTo(TupleId other) {
        int tableCompare = this.tableName.compareTo(other.tableName);
        if (tableCompare != 0) {
            return tableCompare;
        }
        int rowCompare = Long.compare(this.rowId, other.rowId);
        if (rowCompare != 0) {
            return rowCompare;
        }
        return Long.compare(this.version, other.version);
    }

    public static TupleId create(String tableName) {
        return new TupleId(tableName, Instant.now().toEpochMilli());
    }
} 