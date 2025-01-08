package com.easydb.storage.transaction;

import com.easydb.core.TupleId;
import com.easydb.core.Transaction;
import com.easydb.core.IsolationLevel;
import com.easydb.core.LockMode;
import com.easydb.core.Lock;    
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Lock Manager for Higher Isolation Levels
public class LockManager {
    private final ConcurrentMap<TupleId, ReadWriteLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, Set<Lock>> transactionLocks = new ConcurrentHashMap<>();

    public boolean acquireLock(Transaction txn, TupleId tupleId, LockMode mode) {
        if (txn.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED ||
            txn.getIsolationLevel() == IsolationLevel.READ_COMMITTED && mode == LockMode.SHARED) {
            return true; // No locks needed
        }

        ReadWriteLock lock = locks.computeIfAbsent(tupleId, k -> new ReentrantReadWriteLock());
        boolean acquired = false;

        switch (mode) {
            case LockMode.SHARED:
                acquired = lock.readLock().tryLock();
                break;
            case LockMode.EXCLUSIVE:
                acquired = lock.writeLock().tryLock();
                break;
        }

        if (acquired) {
            transactionLocks.computeIfAbsent(txn.getId(), k -> ConcurrentHashMap.newKeySet())
                           .add(new Lock(mode, txn));
        }
        return acquired;
    }

    public void releaseAllLocks(Transaction txn) {
        transactionLocks.remove(txn.getId());
    }
}