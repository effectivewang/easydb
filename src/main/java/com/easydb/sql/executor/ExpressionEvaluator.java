package com.easydb.sql.executor;

import com.easydb.sql.planner.expression.Expression;
import com.easydb.storage.Tuple;
import java.util.Objects;

public class ExpressionEvaluator {
    
    public static Object evaluate(Expression expr, Tuple tuple) {
        if (expr == null) return null;

        return switch (expr.getType()) {
            case COLUMN_REF -> tuple.getValue(expr.getValue());
            case CONSTANT -> expr.getValue();
            case ARITHMETIC -> evaluateArithmetic(expr, tuple);
            
            // Logical operators
            case AND -> evaluateAnd(expr, tuple);
            case OR -> evaluateOr(expr, tuple);
            case NOT -> !((Boolean) evaluate(expr.getLeft(), tuple));
            
            // Comparisons
            case EQUALS -> evaluateComparison(expr, tuple, Objects::equals);
            case NOT_EQUALS -> !evaluateComparison(expr, tuple, Objects::equals);
            case LESS_THAN -> evaluateComparison(expr, tuple, (l, r) -> compareValues(l, r) < 0);
            case GREATER_THAN -> evaluateComparison(expr, tuple, (l, r) -> compareValues(l, r) > 0);
            case LESS_EQUAL -> evaluateComparison(expr, tuple, (l, r) -> compareValues(l, r) <= 0);
            case GREATER_EQUAL -> evaluateComparison(expr, tuple, (l, r) -> compareValues(l, r) >= 0);
            
            default -> throw new IllegalArgumentException("Unsupported expression type: " + expr.getType());
        };
    }

    private static Boolean evaluateAnd(Expression expr, Tuple tuple) {
        // Short-circuit evaluation
        return (Boolean) evaluate(expr.getLeft(), tuple) && 
               (Boolean) evaluate(expr.getRight(), tuple);
    }

    private static Boolean evaluateOr(Expression expr, Tuple tuple) {
        // Short-circuit evaluation
        return (Boolean) evaluate(expr.getLeft(), tuple) || 
               (Boolean) evaluate(expr.getRight(), tuple);
    }

    private static Boolean evaluateComparison(Expression expr, Tuple tuple, 
                                            ComparisonFunction comp) {
        Object left = evaluate(expr.getLeft(), tuple);
        Object right = evaluate(expr.getRight(), tuple);
        
        if (left == null || right == null) return false;
        return comp.compare(left, right);
    }

    private static Object evaluateArithmetic(Expression expr, Tuple tuple) {
        Object left = evaluate(expr.getLeft(), tuple);
        Object right = evaluate(expr.getRight(), tuple);
        
        if (left == null || right == null) return null;
        if (!(left instanceof Number) || !(right instanceof Number)) {
            throw new IllegalArgumentException("Arithmetic requires numeric values");
        }

        Number l = (Number) left;
        Number r = (Number) right;
        return switch (expr.getOperator()) {
            case PLUS -> l.doubleValue() + r.doubleValue();
            case MINUS -> l.doubleValue() - r.doubleValue();
            case MULTIPLY -> l.doubleValue() * r.doubleValue();
            case DIVIDE -> {
                if (r.doubleValue() == 0) throw new ArithmeticException("Division by zero");
                yield l.doubleValue() / r.doubleValue();
            }
            case MODULO -> {
                if (r.doubleValue() == 0) throw new ArithmeticException("Division by zero");
                yield l.doubleValue() % r.doubleValue();
            }
            default -> throw new IllegalArgumentException("Unknown arithmetic operator: " + expr.getOperator());
        };
    }

    @FunctionalInterface
    private interface ComparisonFunction {
        boolean compare(Object left, Object right);
    }

    @SuppressWarnings("unchecked")
    private static int compareValues(Object left, Object right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("Cannot compare null values");
        }

        // If both are numbers, compare their numeric values
        if (left instanceof Number && right instanceof Number) {
            double leftVal = ((Number) left).doubleValue();
            double rightVal = ((Number) right).doubleValue();
            return Double.compare(leftVal, rightVal);
        }

        // If both are strings, use string comparison
        if (left instanceof String && right instanceof String) {
            return ((String) left).compareTo((String) right);
        }

        // If both are of the same comparable type, use natural ordering
        if (left instanceof Comparable && left.getClass().equals(right.getClass())) {
            return ((Comparable<Object>) left).compareTo(right);
        }

        // If types are different but can be coerced to string, compare as strings
        if (left instanceof String || right instanceof String) {
            return left.toString().compareTo(right.toString());
        }

        throw new IllegalArgumentException(
            String.format("Cannot compare values of different types: %s and %s", 
                left.getClass().getSimpleName(), 
                right.getClass().getSimpleName())
        );
    }
} 