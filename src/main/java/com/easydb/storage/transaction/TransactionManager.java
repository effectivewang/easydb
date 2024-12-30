package com.easydb.storage.transaction;

import com.easydb.storage.WriteAheadLog;
import com.easydb.storage.Tuple;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Manages database transactions.
 */
public class TransactionManager {
    private final WriteAheadLog wal;
    private final Map<UUID, Transaction> activeTransactions;
    private final Map<UUID, Set<ReadWriteLock>> transactionLocks;

    public TransactionManager(WriteAheadLog wal) {
        this.wal = wal;
        this.activeTransactions = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
    }

    public Transaction beginTransaction() {
        UUID transactionId = UUID.randomUUID();
        Transaction transaction = new TransactionImpl(transactionId);
        activeTransactions.put(transactionId, transaction);
        transactionLocks.put(transactionId, Collections.synchronizedSet(new HashSet<>()));
        wal.logBegin(transactionId);
        return transaction;
    }

    public void prepareCommit(Transaction transaction) {
        // Write all changes to WAL
        for (Map.Entry<UUID, Tuple> entry : transaction.getWriteSet().entrySet()) {
            Tuple tuple = entry.getValue();
            wal.logInsert(transaction.getId(), tuple.id().tableName(), tuple);
        }
    }

    public void releaseAllLocks(Transaction transaction) {
        Set<ReadWriteLock> heldLocks = transactionLocks.remove(transaction.getId());
        if (heldLocks != null) {
            for (ReadWriteLock lock : heldLocks) {
                lock.writeLock().unlock();
            }
        }
        activeTransactions.remove(transaction.getId());
    }

    public void acquireReadLock(Transaction transaction, ReadWriteLock lock) {
        lock.readLock().lock();
        transactionLocks.computeIfAbsent(transaction.getId(), k -> Collections.synchronizedSet(new HashSet<>())).add(lock);
    }

    public void acquireWriteLock(Transaction transaction, ReadWriteLock lock) {
        lock.writeLock().lock();
        transactionLocks.computeIfAbsent(transaction.getId(), k -> Collections.synchronizedSet(new HashSet<>())).add(lock);
    }
} 