package com.easydb.sql.executor;

import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.sql.executor.QueryExecutorState;
import com.easydb.sql.executor.PlanExecutor;
import com.easydb.sql.planner.QueryPredicate;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.sql.planner.QueryPredicate.PredicateType;
import com.easydb.sql.planner.operation.IndexScanOperation;
import com.easydb.sql.executor.PredicateEvaluator;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class IndexScanExecutor implements PlanExecutor {
    private final IndexScanOperation operation;
    private final Storage storage;
    private final QueryExecutorState state;
    private Iterator<Tuple> tupleIterator;

    public IndexScanExecutor(
            IndexScanOperation operation,
            Storage storage,
            QueryExecutorState state) {
        this.operation = operation;
        this.storage = storage;
        this.state = state;
    }

    @Override
    public void init() {
        // Get table and index information
        String tableName = operation.getRangeTableEntry().getTableName();
        IndexMetadata indexMetadata = operation.getIndexMetadata();
        
        // Extract index search conditions
        Map<String, Object> indexConditions = extractIndexConditions(
            operation.getIndexCondition());
        
        // Perform index lookup
        List<Tuple> tuples = storage.scanTuples(tableName, indexConditions, state.getCurrentTransaction());
        this.tupleIterator = tuples.iterator();
    }

    @Override
    public Optional<Tuple> next() {
        while (tupleIterator.hasNext()) {
            Tuple tuple = tupleIterator.next();
            // Apply any additional filter predicates
            List<Object> values = tuple.getValues();

            if (operation.getExpression() == null || 
                ExpressionEvaluator.evaluate(
                    operation.getExpression(), 
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

    private Map<String, Object> extractIndexConditions(QueryPredicate indexCondition) {
        Map<String, Object> conditions = new HashMap<>();
        if (indexCondition == null) {
            return conditions;
        }

        switch (indexCondition.getPredicateType()) {
            case EQUALS -> {
                conditions.put(indexCondition.getColumn(), indexCondition.getValue());
            }
            case AND -> {
                // For compound index conditions
                for (QueryPredicate subPred : indexCondition.getSubPredicates()) {
                    if (subPred.getPredicateType() == PredicateType.EQUALS) {
                        conditions.put(subPred.getColumn(), subPred.getValue());
                    }
                }
            }
            default -> 
                throw new IllegalStateException(
                    "Unsupported index condition type: " + indexCondition.getPredicateType());
        }
        return conditions;
    }

} 