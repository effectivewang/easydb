package com.easydb.storage;

import java.util.UUID;

/**
 * Interface for Write-Ahead Logging (WAL) operations.
 */
public interface WriteAheadLog {
    /**
     * Logs a transaction commit.
     */
    void logCommit(Long transactionId);

    /**
     * Logs a transaction abort.
     */
    void logAbort(Long transactionId);

    /**
     * Logs a transaction begin.
     */
    void logBegin(Long transactionId);

    /**
     * Logs a tuple insert.
     */
    void logInsert(Long transactionId, String tableName, Tuple tuple);

    /**
     * Logs a tuple update.
     */
    void logUpdate(Long transactionId, String tableName, Tuple oldTuple, Tuple newTuple);

    /**
     * Logs a tuple delete.
     */
    void logDelete(Long transactionId, String tableName, Tuple tuple);
} 