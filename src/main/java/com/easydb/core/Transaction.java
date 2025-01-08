package com.easydb.core;

import com.easydb.core.*;
import java.util.*;

/**
 * Interface for database transactions.
 */
public class Transaction {
    private final Long txnId;
    private final Long beginTs;
    private long commitTs;
    private final Set<TupleId> readSet;
    private final Map<Long, Tuple> writeSet;
    private TransactionStatus status;
    private IsolationLevel isolationLevel;
    private final List<Runnable> cleanupActions;

    public Transaction(Long txnId, Long beginTs, IsolationLevel isolationLevel) {
        this.txnId = txnId;
        this.beginTs = beginTs;
        this.isolationLevel = isolationLevel;
        this.readSet = new HashSet<>();
        this.writeSet = new HashMap<>();
        this.status = TransactionStatus.ACTIVE;
        this.cleanupActions = new ArrayList<>();
    }
    public Long getId() {
        return txnId;
    }

    public void addCleanupAction(Runnable action) {
        cleanupActions.add(action);
    }
    public boolean isCommitted() {
        return status == TransactionStatus.COMMITTED;
    }
    public boolean isAborted() {
        return status == TransactionStatus.ABORTED;
    }
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public long getCommitTs() {
        return commitTs;
    }
    public long getBeginTs() {
        return beginTs;
    }

    public void setCommitTs(long commitTs) {
        this.commitTs = commitTs;
    }

    public Set<TupleId> getReadSet() {
        return readSet;
    }

    public Map<Long, Tuple> getWriteSet() {
        return writeSet;
    }
} 


