package com.easydb.storage.transaction;

import com.easydb.storage.TupleId;
import com.easydb.storage.Tuple;

import java.sql.ResultSet;
import java.util.*;

/**
 * Interface for database transactions.
 */
public class Transaction {
    private final IsolationLevel isolationLevel;
    private final Long id;
    private final Map<Long, Tuple> writeSet;
    private final Map<Long, Tuple> readSet;

    public Transaction(Long id, IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
        this.id = id;
        this.writeSet = new HashMap<>();
        this.readSet = new HashMap<>();
    }

    public Long getId() { return this.id; }
    public IsolationLevel getIsolationLevel() { return this.isolationLevel; }
    public Map<Long, Tuple> getWriteSet() { return this.writeSet; }
    public Map<Long, Tuple> getReadSet() { return this.readSet; }
    
} 