package com.easydb.storage.transaction;

import com.easydb.storage.TupleId;
import com.easydb.storage.Tuple;
import java.util.*;

/**
 * Interface for database transactions.
 */
public interface Transaction {
    UUID getId();
    
    TransactionStatus getStatus();
    
    void setStatus(TransactionStatus status);
    
    void addToWriteSet(Tuple tuple);
    
    Map<UUID, Tuple> getWriteSet();
    
    void addToReadSet(TupleId tupleId);
    
    Set<TupleId> getReadSet();
} 