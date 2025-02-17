package com.easydb.sql.executor;

import com.easydb.sql.planner.QueryTree;
import com.easydb.sql.planner.operation.DeleteOperation;
import com.easydb.sql.planner.expression.Expression;
import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import java.util.Optional;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

public class DeleteExecutor implements PlanExecutor {
    private final DeleteOperation operation;
    private final Storage storage;
    private final QueryExecutorState state;
    private final PlanExecutor childExecutor;
    private Iterator<Tuple> tupleIterator;

    public DeleteExecutor(
            DeleteOperation operation,
            Storage storage,
            QueryExecutorState state,
            PlanExecutor childExecutor) {
        this.operation = operation;
        this.storage = storage;
        this.state = state;
        this.childExecutor = childExecutor;
    }

    @Override
    public void init() {
        childExecutor.init();
        // Collect all tuples from child and store iterator
        List<Tuple> tuples = new ArrayList<>();
        Optional<Tuple> tuple;
        while ((tuple = childExecutor.next()).isPresent()) {
            tuples.add(tuple.get());
        }
        this.tupleIterator = tuples.iterator();
    }

    @Override
    public Optional<Tuple> next() {
        while (tupleIterator.hasNext()) {
            Tuple tuple = tupleIterator.next();
            
            // Double-check WHERE clause if needed
            Expression whereClause = operation.getWhereClause();
            if (whereClause == null || 
                (Boolean) ExpressionEvaluator.evaluate(whereClause, tuple)) {
                
                // Delete tuple (mark as deleted in version chain)
                storage.deleteTuple(
                    tuple.id(),
                    state.getCurrentTransaction()
                );
                
                return Optional.of(tuple);
            }
        }
        return Optional.empty();
    }

    @Override
    public void close() {
        childExecutor.close();
    }

    @Override
    public void rescan() {
        childExecutor.rescan();
        init();  // Re-initialize iterator
    }

    @Override
    public String toString() {
        return String.format("DeleteExecutor: %s WHERE %s", 
            operation.getTargetTable().getTableName(),
            operation.getWhereClause() != null ? operation.getWhereClause() : "true");
    }
} 