package com.easydb.storage;

import java.util.UUID;

/**
 * Represents a unique identifier for a tuple.
 */
public record TupleId(UUID uuid, String tableName) {
    public static TupleId create(String tableName) {
        return new TupleId(UUID.randomUUID(), tableName);
    }
} 