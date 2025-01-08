package com.easydb.storage.transaction;

import com.easydb.core.Tuple;
import com.easydb.core.TupleId;
import com.easydb.core.Transaction;
import com.easydb.core.IsolationLevel;
import com.easydb.core.Lock;
import com.easydb.core.LockMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class VersionStore {
    private final ConcurrentHashMap<TupleId, VersionChain> versions = new ConcurrentHashMap<>();

    public Version getVersion(TupleId tupleId, Transaction txn) {
        VersionChain chain = versions.get(tupleId);
        if (chain == null) return null;

        switch (txn.getIsolationLevel()) {
            case IsolationLevel.READ_UNCOMMITTED:
                return chain.getLatestVersion();
            case IsolationLevel.READ_COMMITTED:
                return chain.getLatestCommittedVersion();
            case IsolationLevel.REPEATABLE_READ:
            case IsolationLevel.SERIALIZABLE:
            case IsolationLevel.SNAPSHOT:       
                return chain.getVersionAtTimestamp(txn.getBeginTs());   
            default:
                throw new IllegalStateException("Unknown isolation level");
        }
    }

    public void clear(TupleId tupleId) {
        versions.remove(tupleId);
    }

    public Set<TupleId> getAllTupleIds() {
        return versions.keySet();
    }

    public void addVersion(TupleId tupleId, Version version) {
        versions.computeIfAbsent(tupleId, k -> new VersionChain()).addVersion(version);
    }

    public void removeVersion(TupleId tupleId, Version version) {
        // TODO: Implement removeVersion
    }

    public Version getLatestVersion(TupleId tupleId) {
        return versions.get(tupleId).getLatestVersion();
    }

    public Version getLatestCommittedVersion(TupleId tupleId) {
        return versions.get(tupleId).getLatestVersion();
    }

    public Version getVersionAtTimestamp(TupleId tupleId, long timestamp) {
        return versions.get(tupleId).getVersionAtTimestamp(timestamp);
    }
}
