package com.easydb.storage.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages database transactions with MVCC support.
 */
public class TransactionManager {
    private final ConcurrentHashMap<Long, Transaction> activeTransactions;
    private final ConcurrentHashMap<Long, TransactionStatus> transactionStatuses;
    private final AtomicLong transactionIdGenerator;

    public TransactionManager() {
        this.activeTransactions = new ConcurrentHashMap<>();
        this.transactionStatuses = new ConcurrentHashMap<>();
        this.transactionIdGenerator = new AtomicLong(1); // Start from 1
    }

    public Transaction beginTransaction(IsolationLevel level) {
        long txnId = transactionIdGenerator.getAndIncrement();
        Transaction txn = new Transaction(txnId, level);
        
        // Set snapshot for isolation
        if (level != IsolationLevel.READ_COMMITTED) {
            Set<Long> activeXids = new HashSet<>(activeTransactions.keySet());
            txn.setActiveTransactionsAtStart(activeXids);
        }
        
        activeTransactions.put(txnId, txn);
        transactionStatuses.put(txnId, TransactionStatus.ACTIVE);
        return txn;
    }

    public void commit(Transaction txn) {
        transactionStatuses.put(txn.getXid(), TransactionStatus.COMMITTED);
        activeTransactions.remove(txn.getXid());
    }

    public void rollback(Transaction txn) {
        transactionStatuses.put(txn.getXid(), TransactionStatus.ABORTED);
        activeTransactions.remove(txn.getXid());
    }

    public boolean isCommitted(long xid) {
        return transactionStatuses.get(xid) == TransactionStatus.COMMITTED;
    }

    public boolean isActive(long xid) {
        return transactionStatuses.get(xid) == TransactionStatus.ACTIVE;
    }
} 