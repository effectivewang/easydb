package com.easydb.sql.planner.operation;

import com.easydb.sql.planner.QueryOperator;
import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.sql.planner.QueryPredicate;

/**
 * Represents a sequential scan operation, similar to PostgreSQL's SeqScan node.
 */
public class SequentialScanOperation extends ScanOperation {
    
    public SequentialScanOperation(RangeTableEntry rte, QueryPredicate predicate) {
        super(rte, predicate);
    }

    @Override
    public QueryOperator getOperator() {
        return QueryOperator.SEQUENTIAL_SCAN;
    }

    @Override
    public String toString() {
        return String.format("SeqScan on %s%s", 
            getRangeTableEntry().getDisplayName(),
            getPredicate() != null ? " Filter: " + getPredicate() : "");
    }
} 