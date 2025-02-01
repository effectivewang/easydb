package com.easydb.sql.planner;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a node in the query execution plan.
 * Each node represents an operation in the query plan, such as table scan,
 * filter, join, sort, etc.
 */
public class QueryTree {
    private final QueryOperator operator;
    private final List<QueryTree> children;
    private final QueryPredicate predicate;
    private final List<String> outputColumns;
    private double estimatedCost;
    private long estimatedRows;

    public QueryTree(QueryOperator operator, QueryPredicate predicate, List<String> outputColumns) {
        this.operator = operator;
        this.predicate = predicate;
        this.outputColumns = new ArrayList<>(outputColumns);
        this.children = new ArrayList<>();
        this.estimatedCost = 0;
        this.estimatedRows = 0;
    }

    public void addChild(QueryTree child) {
        children.add(child);
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public List<QueryTree> getChildren() {
        return new ArrayList<>(children);
    }

    public QueryPredicate getPredicate() {
        return predicate;
    }

    public List<String> getOutputColumns() {
        return new ArrayList<>(outputColumns);
    }

    public double getEstimatedCost() {
        return estimatedCost;
    }

    public void setEstimatedCost(double cost) {
        this.estimatedCost = cost;
    }

    public long getEstimatedRows() {
        return estimatedRows;
    }

    public void setEstimatedRows(long rows) {
        this.estimatedRows = rows;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb, 0);
        return sb.toString();
    }

    private void toString(StringBuilder sb, int indent) {
        sb.append("  ".repeat(indent))
          .append(operator)
          .append(predicate != null ? " [" + predicate + "]" : "")
          .append(" -> ")
          .append(outputColumns)
          .append(" (cost=")
          .append(String.format("%.2f", estimatedCost))
          .append(", rows=")
          .append(estimatedRows)
          .append(")\n");
        
        for (QueryTree child : children) {
            child.toString(sb, indent + 1);
        }
    }
} 