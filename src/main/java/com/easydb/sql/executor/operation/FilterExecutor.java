package com.easydb.sql.executor.operation;

import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.sql.executor.QueryExecutorState;
import com.easydb.sql.executor.PlanExecutor;
import com.easydb.sql.planner.operation.FilterOperation;
import com.easydb.sql.executor.PredicateEvaluator;

import java.util.List;
import java.util.Optional;

/**
 * Executes a filter operation (WHERE clause) in the query plan.
 * Similar to PostgreSQL's FilterNode.
 */
public class FilterExecutor implements PlanExecutor {
    private final FilterOperation operation;
    private final PlanExecutor childExecutor;
    private final QueryExecutorState state;

    public FilterExecutor(
            FilterOperation operation,
            PlanExecutor childExecutor,
            QueryExecutorState state) {
        this.operation = operation;
        this.childExecutor = childExecutor;
        this.state = state;
    }

    @Override
    public void init() {
        // Initialize child executor
        childExecutor.init();
    }

    @Override
    public Optional<Tuple> next() {
        while (true) {
            // Get next tuple from child
            Optional<Tuple> tuple = childExecutor.next();
            if (tuple.isEmpty()) {
                return Optional.empty();
            }

            // Get values for predicate evaluation
            List<Object> values = tuple.get().getValues();

            // Evaluate predicate
            if (PredicateEvaluator.evaluate(
                    operation.getPredicate(),
                    values,
                    operation.getRangeTableEntry())) {
                return tuple;
            }
            // Continue to next tuple if predicate fails
        }
    }

    @Override
    public void close() {
        childExecutor.close();
    }

    @Override
    public void rescan() {
        childExecutor.rescan();
    }
} 