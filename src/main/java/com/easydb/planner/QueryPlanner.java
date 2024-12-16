package com.easydb.planner;

import java.util.concurrent.CompletableFuture;
import java.util.List;

/**
 * Interface for query planning operations in EasyDB.
 */
public interface QueryPlanner {
    /**
     * Create an execution plan for a query.
     *
     * @param query The query to plan
     * @return A future that completes with the execution plan
     */
    CompletableFuture<ExecutionPlan> createPlan(String query);

    /**
     * Optimize an execution plan.
     *
     * @param plan The plan to optimize
     * @return A future that completes with the optimized plan
     */
    CompletableFuture<ExecutionPlan> optimize(ExecutionPlan plan);
}

/**
 * Interface representing an execution plan.
 */
interface ExecutionPlan {
    /**
     * Get the operators in this plan.
     *
     * @return The list of operators
     */
    List<Operator> getOperators();

    /**
     * Get the estimated cost of this plan.
     *
     * @return The estimated cost
     */
    double getEstimatedCost();

    /**
     * Execute this plan.
     *
     * @return A future that completes when execution is done
     */
    CompletableFuture<Void> execute();
}

/**
 * Interface representing a query operator.
 */
interface Operator {
    /**
     * Get the children of this operator.
     *
     * @return The list of child operators
     */
    List<Operator> getChildren();

    /**
     * Get the estimated cost of this operator.
     *
     * @return The estimated cost
     */
    double getEstimatedCost();

    /**
     * Execute this operator.
     *
     * @return A future that completes when execution is done
     */
    CompletableFuture<Void> execute();
} 