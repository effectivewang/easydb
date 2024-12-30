package com.easydb.storage.wal;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages the Write-Ahead Log for durability and recovery.
 */
public class WriteAheadLog implements AutoCloseable {
    private final String logDirectory;
    private final RandomAccessFile logFile;
    private final AtomicLong sequenceNumber;
    private final Map<UUID, List<LogRecord>> transactionLogs;
    private final BlockingQueue<LogRecord> logBuffer;
    private final Thread flushThread;
    private volatile boolean running;

    public WriteAheadLog(String logDirectory) throws IOException {
        this.logDirectory = logDirectory;
        this.logFile = new RandomAccessFile(logDirectory + "/wal.log", "rw");
        this.sequenceNumber = new AtomicLong(0);
        this.transactionLogs = new ConcurrentHashMap<>();
        this.logBuffer = new LinkedBlockingQueue<>();
        this.running = true;

        // Start background flush thread
        this.flushThread = new Thread(this::flushLoop);
        this.flushThread.setDaemon(true);
        this.flushThread.start();
    }

    public CompletableFuture<Void> logBegin(UUID transactionId) {
        return appendLogRecord(new LogRecord(
            transactionId,
            sequenceNumber.incrementAndGet(),
            LogRecord.LogRecordType.BEGIN,
            null,
            null,
            null,
            null,
            Instant.now()
        ));
    }

    public CompletableFuture<Void> logCommit(UUID transactionId) {
        return appendLogRecord(new LogRecord(
            transactionId,
            sequenceNumber.incrementAndGet(),
            LogRecord.LogRecordType.COMMIT,
            null,
            null,
            null,
            null,
            Instant.now()
        ));
    }

    public CompletableFuture<Void> logAbort(UUID transactionId) {
        return appendLogRecord(new LogRecord(
            transactionId,
            sequenceNumber.incrementAndGet(),
            LogRecord.LogRecordType.ABORT,
            null,
            null,
            null,
            null,
            Instant.now()
        ));
    }

    public CompletableFuture<Void> logInsert(UUID transactionId, String tableName, UUID tupleId, byte[] tupleData) {
        return appendLogRecord(new LogRecord(
            transactionId,
            sequenceNumber.incrementAndGet(),
            LogRecord.LogRecordType.INSERT,
            tableName,
            tupleId,
            null,
            tupleData,
            Instant.now()
        ));
    }

    public CompletableFuture<Void> logUpdate(UUID transactionId, String tableName, UUID tupleId, 
                                           byte[] beforeImage, byte[] afterImage) {
        return appendLogRecord(new LogRecord(
            transactionId,
            sequenceNumber.incrementAndGet(),
            LogRecord.LogRecordType.UPDATE,
            tableName,
            tupleId,
            beforeImage,
            afterImage,
            Instant.now()
        ));
    }

    public CompletableFuture<Void> logDelete(UUID transactionId, String tableName, UUID tupleId, byte[] beforeImage) {
        return appendLogRecord(new LogRecord(
            transactionId,
            sequenceNumber.incrementAndGet(),
            LogRecord.LogRecordType.DELETE,
            tableName,
            tupleId,
            beforeImage,
            null,
            Instant.now()
        ));
    }

    private CompletableFuture<Void> appendLogRecord(LogRecord record) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            logBuffer.put(record);
            transactionLogs.computeIfAbsent(record.transactionId(), k -> new ArrayList<>())
                         .add(record);
            future.complete(null);
        } catch (InterruptedException e) {
            future.completeExceptionally(e);
            Thread.currentThread().interrupt();
        }
        return future;
    }

    private void flushLoop() {
        while (running) {
            try {
                List<LogRecord> batch = new ArrayList<>();
                logBuffer.drainTo(batch, 1000); // Batch up to 1000 records
                if (!batch.isEmpty()) {
                    flushBatch(batch);
                } else {
                    Thread.sleep(10); // Sleep if no records to flush
                }
            } catch (Exception e) {
                // Log error but keep running
                e.printStackTrace();
            }
        }
    }

    private void flushBatch(List<LogRecord> batch) throws IOException {
        for (LogRecord record : batch) {
            // Write record type
            logFile.writeByte(record.type().ordinal());
            
            // Write transaction ID
            byte[] txId = record.transactionId().toString().getBytes();
            logFile.writeInt(txId.length);
            logFile.write(txId);
            
            // Write sequence number
            logFile.writeLong(record.sequenceNumber());
            
            // Write table name if present
            if (record.tableName() != null) {
                byte[] tableName = record.tableName().getBytes();
                logFile.writeInt(tableName.length);
                logFile.write(tableName);
            } else {
                logFile.writeInt(0);
            }
            
            // Write tuple ID if present
            if (record.tupleId() != null) {
                byte[] tupleId = record.tupleId().toString().getBytes();
                logFile.writeInt(tupleId.length);
                logFile.write(tupleId);
            } else {
                logFile.writeInt(0);
            }
            
            // Write before image if present
            if (record.beforeImage() != null) {
                logFile.writeInt(record.beforeImage().length);
                logFile.write(record.beforeImage());
            } else {
                logFile.writeInt(0);
            }
            
            // Write after image if present
            if (record.afterImage() != null) {
                logFile.writeInt(record.afterImage().length);
                logFile.write(record.afterImage());
            } else {
                logFile.writeInt(0);
            }
            
            // Write timestamp
            logFile.writeLong(record.timestamp().toEpochMilli());
        }
        logFile.getFD().sync(); // Force flush to disk
    }

    public List<LogRecord> getTransactionLog(UUID transactionId) {
        return transactionLogs.getOrDefault(transactionId, Collections.emptyList());
    }

    public void checkpoint() throws IOException {
        LogRecord checkpoint = new LogRecord(
            UUID.randomUUID(),
            sequenceNumber.incrementAndGet(),
            LogRecord.LogRecordType.CHECKPOINT,
            null,
            null,
            null,
            null,
            Instant.now()
        );
        flushBatch(List.of(checkpoint));
        logFile.setLength(logFile.getFilePointer()); // Truncate old records
        transactionLogs.clear();
    }

    @Override
    public void close() throws IOException {
        running = false;
        try {
            flushThread.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logFile.close();
    }
} 