package com.easydb.sql.executor;

import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.sql.executor.QueryExecutorState;
import com.easydb.sql.executor.PlanExecutor;
import com.easydb.sql.planner.operation.SequentialScanOperation;
import com.easydb.sql.executor.PredicateEvaluator;
import com.easydb.sql.planner.QueryPredicate;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.HashMap;

public class SequentialScanExecutor implements PlanExecutor {
    private final SequentialScanOperation operation;
    private final Storage storage;
    private final QueryExecutorState state;
    private Iterator<Tuple> tupleIterator;

    public SequentialScanExecutor(
            SequentialScanOperation operation,
            Storage storage,
            QueryExecutorState state) {
        this.operation = operation;
        this.storage = storage;
        this.state = state;
    }

    @Override
    public void init() {
        // Get table from range table entry
        String tableName = operation.getRangeTableEntry().getTableName();

        // Convert predicate to condition map
        Map<String, Object> conditions = new HashMap<>();
        // Start table scan with conditions
        List<Tuple> tuples = storage.scanTuples(tableName, conditions, state.getCurrentTransaction());
        this.tupleIterator = tuples.iterator();
    }

    @Override
    public Optional<Tuple> next() {
        while (tupleIterator.hasNext()) {
            Tuple tuple = tupleIterator.next();
            List<Object> values = tuple.getValues();
            
            // Use shared PredicateEvaluator
            if (operation.getPredicate() == null || 
                PredicateEvaluator.evaluate(
                    operation.getPredicate(), 
                    values,
                    operation.getRangeTableEntry())) {
                return Optional.of(tuple);
            }
        }
        return Optional.empty();
    }

    @Override
    public void close() {
        // Cleanup any resources
    }

    @Override
    public void rescan() {
        // Reset iterator to start
        init();
    }
} 