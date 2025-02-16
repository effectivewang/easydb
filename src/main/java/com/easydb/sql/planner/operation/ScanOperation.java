package com.easydb.sql.planner.operation;

import com.easydb.sql.planner.QueryOperator;
import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.sql.planner.QueryPredicate;
import com.easydb.sql.planner.Operation;
/**
 * Base class for scan operations, similar to PostgreSQL's Scan node.
 */
public abstract class ScanOperation implements Operation {
    private final RangeTableEntry rte;
    private final QueryPredicate predicate;  // Filter condition, if any

    protected ScanOperation(RangeTableEntry rte, QueryPredicate predicate) {
        this.rte = rte;
        this.predicate = predicate;
    }

    public RangeTableEntry getRangeTableEntry() {
        return rte;
    }

    public QueryPredicate getPredicate() {
        return predicate;
    }
} 