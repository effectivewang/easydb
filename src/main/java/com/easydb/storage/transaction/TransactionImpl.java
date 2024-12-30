package com.easydb.storage.transaction;

import com.easydb.storage.TupleId;
import com.easydb.storage.Tuple;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of the Transaction interface.
 */
public class TransactionImpl implements Transaction {
    private final UUID id;
    private final Set<TupleId> readSet;
    private final Map<UUID, Tuple> writeSet;
    private TransactionStatus status;

    public TransactionImpl(UUID id) {
        this.id = id;
        this.readSet = Collections.synchronizedSet(new HashSet<>());
        this.writeSet = new ConcurrentHashMap<>();
        this.status = TransactionStatus.ACTIVE;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public TransactionStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    @Override
    public void addToWriteSet(Tuple tuple) {
        writeSet.put(tuple.id().uuid(), tuple);
    }

    @Override
    public Map<UUID, Tuple> getWriteSet() {
        return Collections.unmodifiableMap(writeSet);
    }

    @Override
    public void addToReadSet(TupleId tupleId) {
        readSet.add(tupleId);
    }

    @Override
    public Set<TupleId> getReadSet() {
        return Collections.unmodifiableSet(readSet);
    }
} 