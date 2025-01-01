package com.easydb.storage;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a unique identifier for a tuple.
 */
public class TupleId implements Comparable<TupleId> {
    private final String tableName;
    private final long uuid;

    public static final TupleId MIN = new TupleId("", Long.MIN_VALUE);
    public static final TupleId MAX = new TupleId("", Long.MAX_VALUE);

    public TupleId(String tableName, long id) {
        this.tableName = tableName;
        this.uuid = id;
    }

    public String tableName() {
        return tableName;
    }

    public Long uuid() {
        return uuid;
    }

    @Override
    public int compareTo(TupleId other) {
        int tableCompare = this.tableName.compareTo(other.tableName);
        if (tableCompare != 0) {
            return tableCompare;
        }
        return Long.compare(this.uuid, other.uuid);
    }

    public static TupleId create(String tableName) {
        return new TupleId(tableName, Instant.now().toEpochMilli());
    }
} 