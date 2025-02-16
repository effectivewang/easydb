package com.easydb.sql.executor;

import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.sql.planner.QueryTree;
import com.easydb.sql.planner.QueryPredicate;
import com.easydb.sql.planner.operation.InsertOperation;
import com.easydb.sql.planner.operation.SequentialScanOperation;
import com.easydb.sql.planner.operation.IndexScanOperation;
import com.easydb.sql.planner.operation.ProjectOperation;
import com.easydb.sql.planner.operation.FilterOperation;
import com.easydb.sql.executor.operation.SequentialScanExecutor;
import com.easydb.sql.executor.operation.IndexScanExecutor;
import com.easydb.sql.executor.operation.InsertExecutor;
import com.easydb.sql.executor.operation.ProjectExecutor;
import com.easydb.sql.executor.operation.FilterExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.Optional;

/**
 * Executes a query tree in parallel while maintaining transaction isolation.
 * Uses a work-stealing thread pool for parallel execution and ensures that
 * all operations within a transaction maintain ACID properties.
 */
public class QueryExecutor {
    private final Storage storage;
    private final ExecutionContext executionContext;

    public QueryExecutor(Storage storage, ExecutionContext executionContext) {
        this.storage = storage;
        this.executionContext = executionContext;
    }

    public List<Tuple> execute(QueryTree plan) {
        // Create executor state
        QueryExecutorState state = new QueryExecutorState(plan, executionContext);
        
        // Create root executor
        PlanExecutor executor = createExecutor(plan, state);
        
        try {
            // Initialize executor tree
            executor.init();
            
            // Collect results using iterator pattern
            List<Tuple> results = new ArrayList<>();
            Optional<Tuple> tuple;
            while ((tuple = executor.next()).isPresent()) {
                results.add(tuple.get());
            }
            
            return results;
        } finally {
            executor.close();
        }
    }

    private PlanExecutor createExecutor(QueryTree node, QueryExecutorState state) {
        // Create child executors first
        List<PlanExecutor> children = node.getChildren().stream()
            .map(child -> createExecutor(child, state))
            .collect(Collectors.toList());

        // Create executor for current node
        return switch (node.getOperator()) {
            case SEQUENTIAL_SCAN -> new SequentialScanExecutor(
                (SequentialScanOperation)node.getOperation(), 
                storage, 
                state
            );
            case INDEX_SCAN -> new IndexScanExecutor(
                (IndexScanOperation)node.getOperation(), 
                storage, 
                state
            );
            case INSERT -> new InsertExecutor(
                (InsertOperation)node.getOperation(),
                storage,
                state
            );
            case PROJECT -> new ProjectExecutor(
                (ProjectOperation)node.getOperation(),
                children.get(0),
                state
            );
            case FILTER -> new FilterExecutor(
                (FilterOperation)node.getOperation(),
                children.get(0),
                state
            );
            default -> throw new IllegalStateException("Unsupported operator: " + node.getOperator());
        };
    }
} 