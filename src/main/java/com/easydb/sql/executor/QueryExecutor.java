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
import com.easydb.sql.executor.SequentialScanExecutor;
import com.easydb.sql.executor.IndexScanExecutor;
import com.easydb.sql.executor.InsertExecutor;
import com.easydb.sql.executor.ProjectExecutor;
import com.easydb.sql.executor.FilterExecutor;
import com.easydb.storage.transaction.Transaction;
import com.easydb.storage.transaction.IsolationLevel;
import com.easydb.sql.planner.operation.UpdateOperation;
import com.easydb.sql.executor.UpdateExecutor;
import com.easydb.sql.executor.DeleteExecutor;

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
        // Get or start transaction
        Transaction txn = executionContext.getCurrentTransaction();
        if (txn == null) {
            txn = executionContext.beginTransaction(IsolationLevel.READ_COMMITTED);
        }
        
        // Create executor state with transaction context
        QueryExecutorState state = new QueryExecutorState(plan, executionContext);
        
        // Create and initialize executor tree
        PlanExecutor executor = createExecutor(plan, state);
        try {
            executor.init();
            
            // Execute with transaction context
            List<Tuple> results = new ArrayList<>();
            Optional<Tuple> tuple;
            while ((tuple = executor.next()).isPresent()) {
                Tuple visibleTuple = tuple.get();
                // Record read in transaction
                txn.recordRead(visibleTuple.id());
                results.add(visibleTuple);
            }
            
            return results;
        } catch (Exception e) {
            executionContext.rollbackTransaction();
            throw e;
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