package com.easydb.storage.transaction;

import com.easydb.storage.TupleId;
import com.easydb.storage.Tuple;
import java.util.*;

/**
 * Interface for database transactions.
 */
public interface Transaction {
    Long getId();
    
    TransactionStatus getStatus();
    
    void setStatus(TransactionStatus status);
    
    void addToWriteSet(Tuple tuple);
    
    Map<Long, Tuple> getWriteSet();
    
    void addToReadSet(TupleId tupleId);
    
    Set<TupleId> getReadSet();
} 