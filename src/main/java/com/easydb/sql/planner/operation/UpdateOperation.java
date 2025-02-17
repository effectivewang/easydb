package com.easydb.sql.planner.operation;

import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.sql.planner.expression.Expression;
import com.easydb.sql.planner.Operation;
import com.easydb.sql.planner.QueryOperator;
import java.util.List;
import java.util.Objects;
import java.util.Collections;
import java.util.ArrayList;

public class UpdateOperation implements Operation {
    private final List<String> targetColumns;
    private final List<Expression> setExpressions;
    private final RangeTableEntry targetTable;
    private final Expression whereClause;

    public UpdateOperation(
            List<String> targetColumns,
            List<Expression> setExpressions,
            RangeTableEntry targetTable,
            Expression whereClause) {
        this.targetColumns = Collections.unmodifiableList(new ArrayList<>(
            Objects.requireNonNull(targetColumns, "Target columns cannot be null")));
        this.setExpressions = Collections.unmodifiableList(new ArrayList<>(
            Objects.requireNonNull(setExpressions, "Set expressions cannot be null")));
        this.targetTable = Objects.requireNonNull(targetTable, "Target table cannot be null");
        this.whereClause = whereClause;  // Can be null for updating all rows

        validateExpressions(targetColumns, setExpressions);
    }

    private void validateExpressions(List<String> columns, List<Expression> expressions) {
        if (columns.size() != expressions.size()) {
            throw new IllegalArgumentException(
                String.format("Column count (%d) does not match expression count (%d)",
                            columns.size(), expressions.size()));
        }
    }

    @Override
    public QueryOperator getOperator() {
        return QueryOperator.UPDATE;
    }

    // Immutable getters
    public List<String> getTargetColumns() {
        return targetColumns;
    }

    public List<Expression> getSetExpressions() {
        return setExpressions;
    }

    public RangeTableEntry getTargetTable() {
        return targetTable;
    }

    public Expression getWhereClause() {
        return whereClause;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(targetTable.getTableName());
        sb.append(" SET ");
        for (int i = 0; i < targetColumns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(targetColumns.get(i))
              .append(" = ")
              .append(setExpressions.get(i));
        }
        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }
} 