package com.easydb.sql.executor;

import com.easydb.storage.Tuple;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.sql.planner.QueryTree;
import com.easydb.storage.transaction.Transaction;
import java.util.*;
import com.easydb.sql.planner.RangeTableEntry;
/**
 * Maintains state for a single query execution, similar to PostgreSQL's EState.
 * Each query execution has its own QueryExecutorState instance.
 */
public class QueryExecutorState {
    private final QueryTree queryTree;
    private final ExecutionContext executionContext;
    private final Transaction transaction;
    
    // Per-node intermediate results
    private final Map<Integer, List<Tuple>> intermediateResults;
    
    // Instrument execution (similar to PostgreSQL's instrument.c)
    private final Map<Integer, NodeExecutionStats> executionStats;

    public QueryExecutorState(QueryTree queryTree, ExecutionContext executionContext) {
        this.queryTree = queryTree;
        this.executionContext = executionContext;
        this.transaction = executionContext.getTransaction();
        this.intermediateResults = new HashMap<>();
        this.executionStats = new HashMap<>();
    }

    /**
     * Store intermediate results for a node
     */
    public void storeIntermediateResult(int nodeId, List<Tuple> tuples) {
        long tupleSize = estimateTupleListSize(tuples);
        
        intermediateResults.put(nodeId, tuples);
        
        // Update statistics
        executionStats.computeIfAbsent(nodeId, k -> new NodeExecutionStats())
                     .addTuples(tuples.size());
    }

    /**
     * Retrieve intermediate results for a node
     */
    public List<Tuple> getIntermediateResult(int nodeId) {
        return intermediateResults.getOrDefault(nodeId, Collections.emptyList());
    }


    public Transaction getTransaction() {
        return transaction;
    }

    public NodeExecutionStats getNodeStats(int nodeId) {
        return executionStats.get(nodeId);
    }


    private long estimateTupleListSize(List<Tuple> tuples) {
        return tuples.stream()
                    .mapToLong(this::estimateTupleSize)
                    .sum();
    }

    private long estimateTupleSize(Tuple tuple) {
        RangeTableEntry entry = queryTree.getRangeTable().stream()
            .filter(rte -> rte.getTableName().equals(tuple.id().tableName()))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Table not found: " + tuple.id().tableName()));
        TableMetadata metadata = entry.getMetadata();

        // Basic size estimation
        return tuple.getValues(metadata.columnTypes()).stream()
                   .mapToLong(value -> {
                       if (value instanceof String) {
                           return ((String) value).length() * 2L;
                       }
                       return 8L; // Default size for numbers
                   })
                   .sum();
    }
} 

/**
 * Tracks execution statistics for a single node
 */
class NodeExecutionStats {
    private long tuplesProcessed;
    private long startTime;
    private long endTime;

    public void start() {
        startTime = System.nanoTime();
    }

    public void end() {
        endTime = System.nanoTime();
    }

    public void addTuples(int count) {
        tuplesProcessed += count;
    }

    public long getTuplesProcessed() {
        return tuplesProcessed;
    }

    public double getExecutionTimeMs() {
        return (endTime - startTime) / 1_000_000.0;
    }
}