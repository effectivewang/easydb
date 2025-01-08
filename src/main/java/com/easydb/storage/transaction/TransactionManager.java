package com.easydb.storage.transaction;

import com.easydb.core.Tuple;
import com.easydb.core.TupleId;
import com.easydb.core.Transaction;
import com.easydb.core.IsolationLevel;
import com.easydb.storage.Storage;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

public final class TransactionManager {
    private final VersionStore versionStore;
    private final AtomicLong globalTs;
    private final ConcurrentMap<Long, Transaction> activeTransactions;
    private final LockManager lockManager;
    private final Storage storage;

    public TransactionManager(Storage storage, VersionStore versionStore, AtomicLong globalTs, LockManager lockManager) {
        this.versionStore = versionStore;
        this.globalTs = globalTs;
        this.lockManager = lockManager;
        this.storage = storage;
        this.activeTransactions = new ConcurrentHashMap<>();
    }

    public Transaction beginTransaction(IsolationLevel isolationLevel) {
        Transaction txn = new Transaction(globalTs.incrementAndGet(), Instant.now().toEpochMilli(), isolationLevel);
        activeTransactions.put(txn.getId(), txn);
        return txn;
    }

    public void commit(Transaction txn) {
        try {
            // 1. Validate transaction
            validateTransaction(txn);
            
            // 2. Get commit timestamp
            long commitTs = globalTs.incrementAndGet();
            txn.setCommitTs(commitTs);

            // 3. Update version store with commit timestamp
            for (Map.Entry<Long, Tuple> entry : txn.getWriteSet().entrySet()) {
                Long txnId = entry.getKey();
                Tuple tuple = entry.getValue();
                TupleId tupleId = tuple.id();
                
                // Update version's commit timestamp
                Version version = versionStore.getVersion(tupleId, txn);
                if (version != null) {
                    version.setCommitTs(commitTs);
                }
            }

            // 4. Apply changes to base storage
            applyChangesToStorage(txn);

            // 5. Release all locks
            lockManager.releaseAllLocks(txn);

            // 6. Remove from active transactions
            activeTransactions.remove(txn.getId());

        } catch (Exception e) {
            // If anything fails, rollback
            rollback(txn);
            throw e;
        }
    }

    private void validateTransaction(Transaction txn) {
        // Check for write-write conflicts
        for (Tuple tuple : txn.getWriteSet().values()) {
            Version latest = versionStore.getLatestVersion(tuple.id());
            if (latest != null && latest.getCommitTs() > txn.getBeginTs()) {
                throw new TransactionConflictException("Write-write conflict detected");
            }
        }

        // For SERIALIZABLE: check read set
        if (txn.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
            for (TupleId tupleId : txn.getReadSet()) {
                Version latest = versionStore.getLatestVersion(tupleId);
                if (latest != null && latest.getCommitTs() > txn.getBeginTs()) {
                    throw new TransactionConflictException("Read-write conflict detected");
                }
            }
        }
    }

    public void rollback(Transaction txn) {
        // TODO: Implement rollback
    }

    private void applyChangesToStorage(Transaction txn) {
        // Apply all changes in write set
        for (Map.Entry<Long, Tuple> entry : txn.getWriteSet().entrySet()) {
            Tuple tuple = entry.getValue();
            
            // Signal storage to apply the changes
            storage.applyTransactionChanges(tuple, txn);
        }
    }
    /*
    public CompletableFuture<List<Tuple>> findTuples(
            Transaction txn, 
            SqlCondition condition) {
        return CompletableFuture.supplyAsync(() -> {
            List<VersionChain> chains = versionStore.getVersionChains(condition).join();
            
            return chains.stream()
                .map(chain -> getVisibleVersion(chain, txn))
                .filter(Objects::nonNull)
                .map(Version::getTuple)
                .collect(Collectors.toList());
        });
    }
         */

    private Version getVisibleVersion(VersionChain chain, Transaction txn) {
        switch (txn.getIsolationLevel()) {
            case IsolationLevel.READ_UNCOMMITTED:
                return chain.getLatestVersion();
            case IsolationLevel.READ_COMMITTED:
                return chain.getAllVersions().stream()
                    .filter(v -> v.getCommitTs() > 0)
                    .findFirst()
                    .orElse(null);
            case IsolationLevel.REPEATABLE_READ:
            case IsolationLevel.SERIALIZABLE:
            case IsolationLevel.SNAPSHOT:
                Version version = chain.getVersionAtTimestamp(txn.getBeginTs());
                if (version != null) {
                    txn.getReadSet().add(version.getTupleId());
                }
                return version;
            default:
                throw new IllegalStateException("Unknown isolation level");
        }
    }
}
