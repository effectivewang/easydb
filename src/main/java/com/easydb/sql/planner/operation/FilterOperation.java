package com.easydb.sql.planner.operation;

import com.easydb.sql.planner.QueryOperator;
import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.sql.planner.QueryPredicate;
import com.easydb.sql.planner.Operation;    
import java.util.List;
/**
 * Represents a filter operation (WHERE clause) in the query plan.
 * Similar to PostgreSQL's Filter node.
 */
public class FilterOperation implements Operation {
    private final QueryPredicate predicate;
    private final RangeTableEntry rangeTableEntry;

    public FilterOperation(
            QueryPredicate predicate,
            RangeTableEntry rangeTableEntry) {
        this.predicate = predicate;
        this.rangeTableEntry = rangeTableEntry;
    }

    public QueryPredicate getPredicate() {
        return predicate;
    }

    public RangeTableEntry getRangeTableEntry() {
        return rangeTableEntry;
    }

    @Override
    public QueryOperator getOperator() {
        return QueryOperator.FILTER;
    }

    @Override
    public String toString() {
        return String.format("Filter (%s)", predicate);
    }
} 