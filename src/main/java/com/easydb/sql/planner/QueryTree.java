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
    private final Operation operation;
    private final List<String> outputColumns;
    private final List<QueryTree> children;
    private final List<RangeTableEntry> rangeTable;  // List of referenced tables
    private double estimatedCost;
    private long estimatedRows;

    public QueryTree(
            QueryOperator operator, 
            Operation operation, 
            List<String> outputColumns, 
            List<RangeTableEntry> rangeTable) {
        this.operator = operator;
        this.operation = operation;
        this.outputColumns = new ArrayList<>(outputColumns);
        this.children = new ArrayList<>();
        this.rangeTable = rangeTable != null ? new ArrayList<>(rangeTable) : new ArrayList<>();
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

    public Operation getOperation() {
        return operation;
    }

    public List<String> getOutputColumns() {
        return new ArrayList<>(outputColumns);
    }

    public void addOutputColumn(String column) {
        outputColumns.add(column);
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

    public List<RangeTableEntry> getRangeTable() {
        return new ArrayList<>(rangeTable);
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
          .append("(")
          .append(operation == null ? "null" : operation)
          .append(")")  
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