package com.easydb.sql.executor;

import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.sql.executor.QueryExecutorState;
import com.easydb.sql.executor.PlanExecutor;
import com.easydb.sql.planner.operation.ProjectOperation;
import com.easydb.sql.planner.expression.Expression;
import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.core.Column;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
/**
 * Executes projection operations, similar to PostgreSQL's ProjectionNode
 * Handles column selection and expression evaluation.
 */
public class ProjectExecutor implements PlanExecutor {
    private final ProjectOperation operation;
    private final PlanExecutor childExecutor;
    private final QueryExecutorState state;

    public ProjectExecutor(
            ProjectOperation operation,
            PlanExecutor childExecutor,
            QueryExecutorState state) {
        this.operation = operation;
        this.childExecutor = childExecutor;
        this.state = state;
    }

    @Override
    public void init() {
        childExecutor.init();
    }

    @Override
    public Optional<Tuple> next() {
        // Get next tuple from child
        Optional<Tuple> childTuple = childExecutor.next();
        if (childTuple.isEmpty()) {
            return Optional.empty();
        }

        // Project selected columns
        List<Object> projectedValues = projectTuple(childTuple.get());
        TableMetadata projectedTable = projectTable(childTuple.get().getMetadata());
        // Create new tuple with projected values
        return Optional.of(new Tuple(
            childTuple.get().id(),  // Maintain original tuple ID
            projectedValues,
            childTuple.get().getHeader(),
            childTuple.get().getXmin()
        ));
    }

    @Override
    public void close() {
        childExecutor.close();
    }

    @Override
    public void rescan() {
        childExecutor.rescan();
    }

    private List<Object> projectTuple(Tuple inputTuple) {
        List<Object> result = new ArrayList<>();
        List<String> sourceColumns = operation.getSourceColumns();
        List<Integer> columnIndexes = operation.getColumnIndexes();

        // For each target column
        for (int i = 0; i < operation.getTargetList().size(); i++) {
            if (i < columnIndexes.size()) {
                // Simple column projection
                int sourceIndex = columnIndexes.get(i);
                result.add(inputTuple.getValue(sourceIndex));
            } else {
                // Expression evaluation (for computed columns)
                result.add(evaluateExpression(
                    operation.getExpressions().get(i - columnIndexes.size()),
                    inputTuple
                ));
            }
        }

        return result;
    }

    private TableMetadata projectTable(TableMetadata inputTable) {
        List<String> targetColumnNames = operation.getTargetList();
        List<Integer> columnIndexes = operation.getColumnIndexes();
        
        List<Column> targetColumns = columnIndexes.stream()
            .map(inputTable::getColumn)
            .collect(Collectors.toList());

        return new TableMetadata(
            inputTable.tableName(),
            targetColumns,
            inputTable.indexes(),
            inputTable.constraints()
        );
    }

    private Object evaluateExpression(Expression expr, Tuple tuple) {
        return switch (expr.getType()) {
            case COLUMN_REF -> {
                int columnIndex = operation.findColumnIndex(expr.getLeft().getValue());
                yield tuple.getValue(columnIndex);
            }
            case CONSTANT -> expr.getValue();
            case FUNCTION_CALL -> evaluateFunction(expr, tuple);
            case ARITHMETIC -> evaluateArithmetic(expr, tuple);
            default -> throw new IllegalStateException(
                "Unsupported expression type: " + expr.getType());
        };
    }

    private Object evaluateFunction(Expression expr, Tuple tuple) {
        String functionName = expr.getFunctionName();
        List<Object> args = expr.getArguments().stream()
            .map(arg -> evaluateExpression(arg, tuple))
            .toList();

        return switch (functionName.toLowerCase()) {
            case "upper" -> ((String) args.get(0)).toUpperCase();
            case "lower" -> ((String) args.get(0)).toLowerCase();
            case "concat" -> args.stream()
                .map(String::valueOf)
                .reduce("", String::concat);
            case "length" -> ((String) args.get(0)).length();
            // Add more functions as needed
            default -> throw new IllegalStateException(
                "Unsupported function: " + functionName);
        };
    }

    private Object evaluateArithmetic(Expression expr, Tuple tuple) {
        Object left = evaluateExpression(expr.getLeft(), tuple);
        Object right = evaluateExpression(expr.getRight(), tuple);

        return switch (expr.getOperator()) {
            case PLUS -> add(left, right);
            case MINUS -> subtract(left, right);
            case MULTIPLY -> multiply(left, right);
            case DIVIDE -> divide(left, right);
            default -> throw new IllegalStateException(
                "Unsupported arithmetic operator: " + expr.getOperator());
        };
    }

    private Object add(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return ((Number) left).doubleValue() + ((Number) right).doubleValue();
        }
        throw new IllegalArgumentException("Cannot add non-numeric values");
    }

    private Object subtract(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return ((Number) left).doubleValue() - ((Number) right).doubleValue();
        }
        throw new IllegalArgumentException("Cannot subtract non-numeric values");
    }

    private Object multiply(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return ((Number) left).doubleValue() * ((Number) right).doubleValue();
        }
        throw new IllegalArgumentException("Cannot multiply non-numeric values");
    }

    private Object divide(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            double divisor = ((Number) right).doubleValue();
            if (divisor == 0) {
                throw new ArithmeticException("Division by zero");
            }
            return ((Number) left).doubleValue() / divisor;
        }
        throw new IllegalArgumentException("Cannot divide non-numeric values");
    }
} 