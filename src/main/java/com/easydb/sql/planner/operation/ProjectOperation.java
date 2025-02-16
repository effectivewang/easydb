package com.easydb.sql.planner.operation;

import com.easydb.sql.planner.QueryOperator;
import com.easydb.sql.planner.Operation;
import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.sql.planner.expression.Expression;
import java.util.List;

/**
 * Represents a projection operation, similar to PostgreSQL's Project node.
 * Can project columns from multiple input relations (e.g., after JOINs).
 */
public class ProjectOperation implements Operation {
    private final List<String> targetList;      // Output column names
    private final List<String> sourceColumns;   // Input column names (qualified)
    private final List<Integer> columnIndexes;  // Mapping to source columns
    private final List<Expression> expressions; // Computed expressions
    private final List<RangeTableEntry> rangeTable; // All available input relations

    public ProjectOperation(
            List<String> targetList,
            List<String> sourceColumns,
            List<Integer> columnIndexes,
            List<Expression> expressions,
            List<RangeTableEntry> rangeTable) {
        this.targetList = targetList;
        this.sourceColumns = sourceColumns;
        this.columnIndexes = columnIndexes;
        this.expressions = expressions != null ? expressions : List.of();
        this.rangeTable = rangeTable;
    }

    public List<String> getTargetList() {
        return targetList;
    }

    public List<String> getSourceColumns() {
        return sourceColumns;
    }

    public List<Integer> getColumnIndexes() {
        return columnIndexes;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public List<RangeTableEntry> getRangeTable() {
        return rangeTable;
    }

    public RangeTableEntry findRteForColumn(String columnName) {
        // Handle qualified column names (e.g., "table.column")
        String[] parts = columnName.split("\\.");
        if (parts.length == 2) {
            String tableAlias = parts[0];
            String column = parts[1];
            return rangeTable.stream()
                .filter(rte -> rte.getAlias().equals(tableAlias) && rte.hasColumn(column))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                    "Column not found: " + columnName));
        }

        // Handle unqualified column names
        List<RangeTableEntry> matches = rangeTable.stream()
            .filter(rte -> rte.hasColumn(columnName))
            .toList();

        if (matches.isEmpty()) {
            throw new IllegalStateException("Column not found: " + columnName);
        }
        if (matches.size() > 1) {
            throw new IllegalStateException("Ambiguous column reference: " + columnName);
        }
        return matches.get(0);
    }

    public int findColumnIndex(String columnName) {
        String[] parts = columnName.split("\\.");
        String actualColumn = parts.length == 2 ? parts[1] : columnName;
        return findRteForColumn(columnName).getColumnIndex(actualColumn);   
    }

    public int getColumnIndex(String columnName) {
        return getSourceColumns().indexOf(columnName);
    }

    @Override
    public QueryOperator getOperator() {
        return QueryOperator.PROJECT;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Project (");
        for (int i = 0; i < targetList.size(); i++) {
            if (i > 0) sb.append(", ");
            if (i < columnIndexes.size()) {
                sb.append(sourceColumns.get(columnIndexes.get(i)))
                  .append(" AS ")
                  .append(targetList.get(i));
            } else {
                sb.append(expressions.get(i - columnIndexes.size()))
                  .append(" AS ")
                  .append(targetList.get(i));
            }
        }
        sb.append(")");
        return sb.toString();
    }
} 