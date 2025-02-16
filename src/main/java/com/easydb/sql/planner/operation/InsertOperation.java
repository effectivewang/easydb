package com.easydb.sql.planner.operation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import com.easydb.sql.planner.Operation;
import com.easydb.sql.planner.QueryOperator;

/**
 * Represents an INSERT operation in the query plan.
 * Follows Single Responsibility Principle by handling only insert operations.
 */
public class InsertOperation implements Operation {
    private final String tableName;
    private final List<String> columns;
    private final List<List<Object>> values;

    public InsertOperation(String tableName, List<String> columns, List<List<Object>> values) {
        this.tableName = Objects.requireNonNull(tableName, "Table name cannot be null");
        this.columns = Collections.unmodifiableList(new ArrayList<>(
            Objects.requireNonNull(columns, "Columns cannot be null")));
        this.values = Collections.unmodifiableList(new ArrayList<>(
            Objects.requireNonNull(values, "Values cannot be null")));
        
        // Validate values structure
        validateValues(values, columns.size());
    }

    private void validateValues(List<List<Object>> values, int columnCount) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Values list cannot be empty");
        }
        
        for (List<Object> row : values) {
            if (row.size() != columnCount) {
                throw new IllegalArgumentException(
                    String.format("Value count (%d) does not match column count (%d)", 
                                row.size(), columnCount));
            }
        }
    }

    @Override
    public QueryOperator getOperator() {
        return QueryOperator.INSERT;
    }

    // Immutable getters
    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;  // Already unmodifiable
    }

    public List<List<Object>> getValues() {
        return values;  // Already unmodifiable
    }

    public int getRowCount() {
        return values.size();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ")
          .append(tableName)
          .append(" (")
          .append(String.join(", ", columns))
          .append(") VALUES ");

        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("(")
              .append(String.join(", ", 
                  values.get(i).stream()
                      .map(value -> value == null ? "null" : value.toString())
                      .toList()))
              .append(")");
        }

        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InsertOperation)) return false;
        InsertOperation that = (InsertOperation) o;
        return Objects.equals(tableName, that.tableName) &&
               Objects.equals(columns, that.columns) &&
               Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columns, values);
    }
} 