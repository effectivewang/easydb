package com.easydb.storage.transaction;

import com.easydb.storage.TupleId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a database transaction with MVCC support.
 * Works with TransactionManager for version management.
 */
public class Transaction {
    private final long xid;
    private final AtomicReference<TransactionStatus> status;
    private IsolationLevel isolationLevel;
    
    // Track tuple IDs that this transaction has read/written
    private final Set<TupleId> readSet;
    private final Set<TupleId> writeSet;
    
    // Snapshot information for repeatable read and serializable
    private final long snapshotTimestamp;
    private final Set<Long> activeTransactionsAtStart;

    public Transaction(long xid, IsolationLevel isolationLevel) {
        this.xid = xid;
        this.isolationLevel = isolationLevel;
        this.status = new AtomicReference<>(TransactionStatus.ACTIVE);
        this.readSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.writeSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.snapshotTimestamp = System.nanoTime();
        this.activeTransactionsAtStart = new HashSet<>(); // Set by TransactionManager
    }

    /**
     * Records a tuple read by this transaction.
     */
    public void recordRead(TupleId tupleId) {
        if (requiresReadTracking()) {
            readSet.add(tupleId);
        }
    }

    /**
     * Records a tuple written by this transaction.
     */
    public void recordWrite(TupleId tupleId) {
        writeSet.add(tupleId);
    }

    public void setIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    /**
     * Checks if the transaction needs to track reads based on isolation level.
     */
    private boolean requiresReadTracking() {
        return isolationLevel == IsolationLevel.REPEATABLE_READ || 
               isolationLevel == IsolationLevel.SERIALIZABLE;
    }

    /**
     * Checks if a tuple version is visible to this transaction.
     */
    public boolean isVisible(long xmin, long xmax) {
        switch (isolationLevel) {
            case READ_COMMITTED:
                return xmin < xid && (xmax == 0 || xmax > xid);
            case REPEATABLE_READ:
            case SERIALIZABLE:
                return xmin < xid && 
                       !activeTransactionsAtStart.contains(xmin) &&
                       (xmax == 0 || xmax > xid || activeTransactionsAtStart.contains(xmax));
            default:
                throw new IllegalStateException("Unknown isolation level: " + isolationLevel);
        }
    }

    /**
     * Attempts to commit the transaction.
     */
    public boolean commit() {
        return status.compareAndSet(TransactionStatus.ACTIVE, 
                                  TransactionStatus.COMMITTED);
    }

    /**
     * Attempts to rollback the transaction.
     */
    public boolean rollback() {
        return status.compareAndSet(TransactionStatus.ACTIVE, 
                                  TransactionStatus.ABORTED);
    }

    /**
     * Sets the active transactions at start for snapshot isolation.
     */
    public void setActiveTransactionsAtStart(Set<Long> activeTxns) {
        if (requiresReadTracking()) {
            this.activeTransactionsAtStart.addAll(activeTxns);
        }
    }

    public boolean isCommitted(long xmin) {
        return xmin < xid;
    }

    public boolean wasActiveAtSnapshot(long xmin) {
        return xmin < xid;
    }
    
    // Status checks
    public boolean isActive() {
        return status.get() == TransactionStatus.ACTIVE;
    }

    public boolean isCommitted() {
        return status.get() == TransactionStatus.COMMITTED;
    }

    public boolean isAborted() {
        return status.get() == TransactionStatus.ABORTED;
    }

    // Getters
    public long getXid() { return xid; }
    public IsolationLevel getIsolationLevel() { return isolationLevel; }
    public TransactionStatus getStatus() { return status.get(); }
    public Set<TupleId> getReadSet() { return Collections.unmodifiableSet(readSet); }
    public Set<TupleId> getWriteSet() { return Collections.unmodifiableSet(writeSet); }
    public long getSnapshotTimestamp() { return snapshotTimestamp; }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Transaction)) return false;
        Transaction that = (Transaction) o;
        return xid == that.xid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xid);
    }

    @Override
    public String toString() {
        return String.format("Transaction{xid=%d, status=%s, isolation=%s}", 
                           xid, status.get(), isolationLevel);
    }
} 