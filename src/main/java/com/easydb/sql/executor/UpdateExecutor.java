package com.easydb.sql.executor;

import com.easydb.sql.planner.QueryTree;
import com.easydb.sql.planner.operation.UpdateOperation;
import com.easydb.sql.planner.expression.Expression;
import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.Iterator;
import java.util.ArrayList;

public class UpdateExecutor implements PlanExecutor {
    private final UpdateOperation operation;
    private final Storage storage;
    private final QueryExecutorState state;
    private final PlanExecutor childExecutor;
    private Iterator<Tuple> tupleIterator;

    public UpdateExecutor(
            UpdateOperation operation,
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
            if (whereClause == null || (Boolean) ExpressionEvaluator.evaluate(whereClause, tuple)) {
                // Evaluate SET expressions
                Map<String, Object> updates = evaluateSetExpressions(tuple);

                Tuple updatedTuple = tuple.withUpdatedValues(updates, state.getCurrentTransaction().getXid());
                
                // Update tuple with new version
                storage.updateTuple(
                    updatedTuple.id(),
                    updatedTuple.getValues(),
                    state.getCurrentTransaction()
                );
                
                return Optional.of(updatedTuple);
            }
        }
        return Optional.empty();
    }

    private Map<String, Object> evaluateSetExpressions(Tuple tuple) {
        Map<String, Object> updates = new HashMap<>();
        List<String> targetColumns = operation.getTargetColumns();
        List<Expression> setExpressions = operation.getSetExpressions();

        for (int i = 0; i < targetColumns.size(); i++) {
            String column = targetColumns.get(i);
            Expression expr = setExpressions.get(i);
            Object value = ExpressionEvaluator.evaluate(expr, tuple);
            updates.put(column, value);
        }
        return updates;
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
        return String.format("UpdateExecutor: %s SET %s WHERE %s",
            operation.getTargetTable().getTableName(),
            formatSetClause(),
            operation.getWhereClause() != null ? operation.getWhereClause() : "true");
    }

    private String formatSetClause() {
        StringBuilder sb = new StringBuilder();
        List<String> columns = operation.getTargetColumns();
        List<Expression> expressions = operation.getSetExpressions();
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columns.get(i))
              .append(" = ")
              .append(expressions.get(i));
        }
        return sb.toString();
    }
} 