package com.easydb.sql.planner.operation;

import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.sql.planner.expression.Expression;
import com.easydb.sql.planner.Operation;
import com.easydb.sql.planner.QueryOperator;
import java.util.Objects;

public class DeleteOperation implements Operation {
    private final RangeTableEntry targetTable;
    private final Expression whereClause;  // Filter expression to identify tuples to delete

    public DeleteOperation(RangeTableEntry targetTable, Expression whereClause) {
        this.targetTable = Objects.requireNonNull(targetTable, "Target table cannot be null");
        this.whereClause = whereClause;  // Can be null for deleting all rows
    }

    @Override
    public QueryOperator getOperator() {
        return QueryOperator.DELETE;
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
        sb.append("DELETE FROM ").append(targetTable.getTableName());
        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeleteOperation)) return false;
        DeleteOperation that = (DeleteOperation) o;
        return Objects.equals(targetTable, that.targetTable) &&
               Objects.equals(whereClause, that.whereClause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetTable, whereClause);
    }
} 