package com.easydb.storage.wal;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a single log record in the Write-Ahead Log.
 */
public record LogRecord(
    UUID transactionId,
    long sequenceNumber,
    LogRecordType type,
    String tableName,
    UUID tupleId,
    byte[] beforeImage,
    byte[] afterImage,
    Instant timestamp
) {
    public enum LogRecordType {
        BEGIN,
        COMMIT,
        ABORT,
        INSERT,
        UPDATE,
        DELETE,
        CHECKPOINT
    }
} 