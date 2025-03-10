package com.easydb.sql.executor;

import com.easydb.storage.transaction.Transaction;
import com.easydb.storage.transaction.TransactionManager;
import com.easydb.storage.transaction.IsolationLevel;
import java.util.concurrent.*;
import java.util.Set;
import java.util.stream.Collectors;

public class ExecutionContext {
    private final TransactionManager transactionManager;
    private final ThreadLocal<Transaction> currentTransaction;
    private final Set<Transaction> activeTransactions;
    private IsolationLevel isolationLevel;
    
    public ExecutionContext(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        this.currentTransaction = new ThreadLocal<>();
        this.activeTransactions = ConcurrentHashMap.newKeySet();
        this.isolationLevel = IsolationLevel.READ_COMMITTED;
    }

    /**
     * Starts a new transaction with specified isolation level
     */
    public Transaction beginTransaction() {
        Transaction txn = transactionManager.beginTransaction(isolationLevel);
        currentTransaction.set(txn);
        
        // Track active transactions for isolation
        activeTransactions.add(txn);
        
        return txn;
    }

    public void setIsolationLevel(IsolationLevel level) {
        this.isolationLevel = level;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * Gets current transaction in thread-safe manner
     */
    public Transaction getCurrentTransaction() {
        Transaction txn = currentTransaction.get();
        if (txn == null) {
            throw new IllegalStateException("No active transaction in current thread");
        }
        return txn;
    }

    /**
     * Commits current transaction with proper cleanup
     */
    public void commitTransaction() {
        Transaction txn = getCurrentTransaction();
        try {
            transactionManager.commit(txn);
        } finally {
            cleanup(txn);
        }
    }

    /**
     * Rolls back current transaction with proper cleanup
     */
    public void rollbackTransaction() {
        Transaction txn = getCurrentTransaction();
        try {
            transactionManager.rollback(txn);
        } finally {
            cleanup(txn);
        }
    }

    /**
     * Ensures isolation level guarantees are met
     */
    public boolean checkVisibility(long xmin, long xmax) {
        Transaction txn = getCurrentTransaction();
        return txn.isVisible(xmin, xmax);
    }

    /**
     * Releases resources when transaction completes
     */
    private void cleanup(Transaction txn) {
        currentTransaction.remove();
        activeTransactions.remove(txn);
    }

    /**
     * Gets active transaction IDs for snapshot isolation
     */
    public Set<Long> getActiveTransactionIds() {
        return activeTransactions.stream()
            .map(Transaction::getXid)
            .collect(Collectors.toSet());
    }
}
