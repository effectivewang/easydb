package com.easydb.storage;

import java.util.UUID;

/**
 * Interface for Write-Ahead Logging (WAL) operations.
 */
public interface WriteAheadLog {
    /**
     * Logs a transaction commit.
     */
    void logCommit(UUID transactionId);

    /**
     * Logs a transaction abort.
     */
    void logAbort(UUID transactionId);

    /**
     * Logs a transaction begin.
     */
    void logBegin(UUID transactionId);

    /**
     * Logs a tuple insert.
     */
    void logInsert(UUID transactionId, String tableName, Tuple tuple);

    /**
     * Logs a tuple update.
     */
    void logUpdate(UUID transactionId, String tableName, Tuple oldTuple, Tuple newTuple);

    /**
     * Logs a tuple delete.
     */
    void logDelete(UUID transactionId, String tableName, Tuple tuple);
} 