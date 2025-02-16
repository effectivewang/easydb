package com.easydb.sql.executor;

import com.easydb.sql.planner.QueryPredicate;
import com.easydb.sql.planner.QueryPredicate.PredicateType;
import com.easydb.sql.planner.RangeTableEntry;
import java.util.List;
import java.util.Objects;

/**
 * Utility class for evaluating predicates, shared across executors.
 * Similar to PostgreSQL's qual.c for predicate evaluation.
 */
public class PredicateEvaluator {
    
    public static boolean evaluate(QueryPredicate predicate, List<Object> values, RangeTableEntry rte) {
        if (predicate == null) {
            return true;
        }

        return switch (predicate.getPredicateType()) {
            case EQUALS -> evaluateEquals(predicate, values, rte);
            case NOT_EQUALS -> !evaluateEquals(predicate, values, rte);
            case LESS_THAN -> evaluateComparison(predicate, values, rte, (a, b) -> compare(a, b) < 0);
            case GREATER_THAN -> evaluateComparison(predicate, values, rte, (a, b) -> compare(a, b) > 0);
            case LESS_THAN_OR_EQUALS -> evaluateComparison(predicate, values, rte, (a, b) -> compare(a, b) <= 0);
            case GREATER_THAN_OR_EQUALS -> evaluateComparison(predicate, values, rte, (a, b) -> compare(a, b) >= 0);
            case AND -> evaluateAnd(predicate, values, rte);
            case OR -> evaluateOr(predicate, values, rte);
            case NOT -> !evaluate(predicate.getSubPredicates().get(0), values, rte);
            case IS_NULL -> evaluateIsNull(predicate, values, rte);
            default -> throw new IllegalStateException(
                "Unsupported predicate type: " + predicate.getPredicateType());
        };
    }

    private static boolean evaluateEquals(QueryPredicate predicate, List<Object> values, RangeTableEntry rte) {
        int columnIndex = findColumnIndex(predicate.getColumn(), rte);
        Object tupleValue = values.get(columnIndex);
        return Objects.equals(tupleValue, predicate.getValue());
    }

    private static boolean evaluateAnd(QueryPredicate predicate, List<Object> values, RangeTableEntry rte) {
        return predicate.getSubPredicates().stream()
            .allMatch(subPred -> evaluate(subPred, values, rte));
    }

    private static boolean evaluateOr(QueryPredicate predicate, List<Object> values, RangeTableEntry rte) {
        return predicate.getSubPredicates().stream()
            .anyMatch(subPred -> evaluate(subPred, values, rte));
    }

    private static boolean evaluateIsNull(QueryPredicate predicate, List<Object> values, RangeTableEntry rte) {
        int columnIndex = findColumnIndex(predicate.getColumn(), rte);
        return values.get(columnIndex) == null;
    }

    private static boolean evaluateComparison(
            QueryPredicate predicate, 
            List<Object> values, 
            RangeTableEntry rte,
            ComparisonOperator operator) {
        int columnIndex = findColumnIndex(predicate.getColumn(), rte);
        Object tupleValue = values.get(columnIndex);
        Object predicateValue = predicate.getValue();
        
        if (tupleValue == null || predicateValue == null) {
            return false;  // NULL comparison always returns false
        }
        
        return operator.compare(tupleValue, predicateValue);
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

    @FunctionalInterface
    private interface ComparisonOperator {
        boolean compare(Object a, Object b);
    }
} 