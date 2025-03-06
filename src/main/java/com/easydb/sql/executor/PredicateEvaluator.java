package com.easydb.sql.executor;

import com.easydb.sql.planner.expression.Expression;
import com.easydb.sql.planner.expression.Expression.ExpressionType;
import com.easydb.sql.planner.expression.Expression.Operator;
import com.easydb.sql.planner.RangeTableEntry;
import java.util.List;
import java.util.Objects;

/**
 * Utility class for evaluating expressions, shared across executors.
 * Similar to PostgreSQL's execQual.c for expression evaluation.
 */
public class PredicateEvaluator {
    
    public static Object evaluate(Expression expr, List<Object> values, RangeTableEntry rte) {
        if (expr == null) {
            return true;
        }

        return switch (expr.getType()) {
            case COLUMN_REF -> evaluateColumnRef(expr, values, rte);
            case CONSTANT -> expr.getValue();
            case COMPARISON -> evaluateComparison(expr, values, rte);
            case LOGICAL -> evaluateLogical(expr, values, rte);
            case NOT -> !(Boolean)evaluate(expr.getLeft(), values, rte);
            case ARITHMETIC -> evaluateArithmetic(expr, values, rte);
            case FUNCTION_CALL -> evaluateFunction(expr, values, rte);
        };
    }

    private static Object evaluateColumnRef(Expression expr, List<Object> values, RangeTableEntry rte) {
        String columnName = expr.getValue().toString();
        int columnIndex = findColumnIndex(columnName, rte);
        return values.get(columnIndex);
    }

    private static boolean evaluateComparison(Expression expr, List<Object> values, RangeTableEntry rte) {
        Object leftValue = evaluate(expr.getLeft(), values, rte);
        Object rightValue = evaluate(expr.getRight(), values, rte);
        
        if (leftValue == null || rightValue == null) {
            return false;  // NULL comparison always returns false
        }

        return switch (expr.getOperator()) {
            case EQUALS -> Objects.equals(leftValue, rightValue);
            case NOT_EQUALS -> !Objects.equals(leftValue, rightValue);
            case LESS_THAN -> compare(leftValue, rightValue) < 0;
            case GREATER_THAN -> compare(leftValue, rightValue) > 0;
            case LESS_EQUAL -> compare(leftValue, rightValue) <= 0;
            case GREATER_EQUAL -> compare(leftValue, rightValue) >= 0;
            default -> throw new IllegalStateException("Unsupported comparison operator: " + expr.getOperator());
        };
    }

    private static boolean evaluateLogical(Expression expr, List<Object> values, RangeTableEntry rte) {
        Object leftValue = evaluate(expr.getLeft(), values, rte);
        Object rightValue = evaluate(expr.getRight(), values, rte);
        
        if (!(leftValue instanceof Boolean) || !(rightValue instanceof Boolean)) {
            throw new IllegalStateException("Logical operators require boolean operands");
        }

        return switch (expr.getOperator()) {
            case AND -> (Boolean)leftValue && (Boolean)rightValue;
            case OR -> (Boolean)leftValue || (Boolean)rightValue;
            default -> throw new IllegalStateException("Unsupported logical operator: " + expr.getOperator());
        };
    }

    private static Object evaluateArithmetic(Expression expr, List<Object> values, RangeTableEntry rte) {
        Object leftValue = evaluate(expr.getLeft(), values, rte);
        Object rightValue = evaluate(expr.getRight(), values, rte);
        
        if (leftValue == null || rightValue == null) {
            return null;  // NULL arithmetic returns NULL
        }

        if (!(leftValue instanceof Number) || !(rightValue instanceof Number)) {
            throw new IllegalStateException("Arithmetic operators require numeric operands");
        }

        double left = ((Number)leftValue).doubleValue();
        double right = ((Number)rightValue).doubleValue();

        return switch (expr.getOperator()) {
            case PLUS -> left + right;
            case MINUS -> left - right;
            case MULTIPLY -> left * right;
            case DIVIDE -> {
                if (right == 0) {
                    throw new ArithmeticException("Division by zero");
                }
                yield left / right;
            }
            default -> throw new IllegalStateException("Unsupported arithmetic operator: " + expr.getOperator());
        };
    }

    private static Object evaluateFunction(Expression expr, List<Object> values, RangeTableEntry rte) {
        String functionName = expr.getFunctionName();
        List<Object> args = expr.getArguments().stream()
            .map(arg -> evaluate(arg, values, rte))
            .toList();

        return switch (functionName.toUpperCase()) {
            case "COUNT" -> args.stream().filter(Objects::nonNull).count();
            case "SUM" -> args.stream()
                .filter(Objects::nonNull)
                .mapToDouble(arg -> ((Number)arg).doubleValue())
                .sum();
            case "AVG" -> args.stream()
                .filter(Objects::nonNull)
                .mapToDouble(arg -> ((Number)arg).doubleValue())
                .average()
                .orElse(0.0);
            case "MAX" -> args.stream()
                .filter(Objects::nonNull)
                .max((a, b) -> compare(a, b))
                .orElse(null);
            case "MIN" -> args.stream()
                .filter(Objects::nonNull)
                .min((a, b) -> compare(a, b))
                .orElse(null);
            default -> throw new IllegalStateException("Unsupported function: " + functionName);
        };
    }

    private static int findColumnIndex(String columnName, RangeTableEntry rte) {
        return rte.getMetadata().columnNames().indexOf(columnName);
    }

    @SuppressWarnings("unchecked")
    private static int compare(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable<Object>) a).compareTo(b);
        }
        throw new IllegalArgumentException("Values must be comparable");
    }
} 