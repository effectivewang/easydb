package com.easydb.storage;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Global TupleId generator using static methods.
 * Provides globally unique IDs across all tables in the database.
 */
public class TupleIdGenerator {
    // Singleton instance for global state
    private static final TupleIdGenerator INSTANCE = new TupleIdGenerator();
    
    // Global state for ID generation
    private final AtomicLong lastTimestamp;
    private final AtomicLong sequence;
    
    private TupleIdGenerator() {
        this.lastTimestamp = new AtomicLong(System.currentTimeMillis());
        this.sequence = new AtomicLong(0);
    }

    /**
     * Generates a new globally unique TupleId for the given table.
     * This static method can be called from anywhere in the codebase.
     */
    public static TupleId nextId(String tableName) {
        return INSTANCE.generateId(tableName);
    }

    /**
     * Internal method to generate the next ID
     */
    private TupleId generateId(String tableName) {
        while (true) {
            long timestamp = System.currentTimeMillis();
            long lastTs = lastTimestamp.get();
            
            if (timestamp < lastTs) {
                timestamp = lastTs;
            }
            
            if (timestamp == lastTs) {
                long seq = sequence.incrementAndGet() & 0xFFFF;
                if (seq == 0) {
                    continue;
                }
                return constructId(tableName, timestamp, seq);
            } else {
                if (lastTimestamp.compareAndSet(lastTs, timestamp)) {
                    sequence.set(0);
                    return constructId(tableName, timestamp, 0);
                }
            }
        }
    }

    /**
     * Constructs a TupleId from timestamp and sequence.
     * Combines timestamp and sequence into a single long value.
     */
    private static TupleId constructId(String tableName, long timestamp, long sequence) {
        // Combine timestamp and sequence into a single 64-bit value
        long combinedId = (timestamp << 16) | sequence;
        return new TupleId(tableName, combinedId);
    }

    /**
     * Utility method to extract timestamp from a rowId
     */
    public static long getTimestamp(long rowId) {
        return rowId >>> 16;
    }

    /**
     * Utility method to extract sequence from a rowId
     */
    public static int getSequence(long rowId) {
        return (int)(rowId & 0xFFFF);
    }

    /**
     * Debug method to print ID components
     */
    public static String debugString(TupleId id) {
        return String.format("TupleId{table=%s, ts=%d, seq=%d, ver=%d}",
            id.tableName(),
            getTimestamp(id.rowId()),
            getSequence(id.rowId()),
            id.version()
        );
    }
}