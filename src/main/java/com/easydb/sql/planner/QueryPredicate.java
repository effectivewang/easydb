package com.easydb.sql.planner;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a predicate (filter condition) in a query plan.
 * Can be a simple comparison or a complex boolean expression.
 */
public class QueryPredicate {
    private final PredicateType type;
    private final String column;
    private final Object value;
    private final List<QueryPredicate> subPredicates;

    private QueryPredicate(PredicateType type, String column, Object value, List<QueryPredicate> subPredicates) {
        this.type = type;
        this.column = column;
        this.value = value;
        this.subPredicates = subPredicates != null ? new ArrayList<>(subPredicates) : new ArrayList<>();
    }

    // Factory methods for different types of predicates
    public static QueryPredicate equals(String column, Object value) {
        return new QueryPredicate(PredicateType.EQUALS, column, value, null);
    }

    public static QueryPredicate notEquals(String column, Object value) {
        return new QueryPredicate(PredicateType.NOT_EQUALS, column, value, null);
    }

    public static QueryPredicate lessThan(String column, Object value) {
        return new QueryPredicate(PredicateType.LESS_THAN, column, value, null);
    }

    public static QueryPredicate lessThanEquals(String column, Object value) {
        return new QueryPredicate(PredicateType.LESS_THAN_EQUALS, column, value, null);
    }

    public static QueryPredicate greaterThan(String column, Object value) {
        return new QueryPredicate(PredicateType.GREATER_THAN, column, value, null);
    }

    public static QueryPredicate greaterThanEquals(String column, Object value) {
        return new QueryPredicate(PredicateType.GREATER_THAN_EQUALS, column, value, null);
    }

    public static QueryPredicate isNull(String column) {
        return new QueryPredicate(PredicateType.IS_NULL, column, null, null);
    }

    public static QueryPredicate isNotNull(String column) {
        return new QueryPredicate(PredicateType.IS_NOT_NULL, column, null, null);
    }

    public static QueryPredicate and(List<QueryPredicate> predicates) {
        return new QueryPredicate(PredicateType.AND, null, null, predicates);
    }

    public static QueryPredicate or(List<QueryPredicate> predicates) {
        return new QueryPredicate(PredicateType.OR, null, null, predicates);
    }

    public static QueryPredicate not(QueryPredicate predicate) {
        List<QueryPredicate> predicates = new ArrayList<>();
        predicates.add(predicate);
        return new QueryPredicate(PredicateType.NOT, null, null, predicates);
    }

    // Getters
    public PredicateType getType() {
        return type;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    public List<QueryPredicate> getSubPredicates() {
        return new ArrayList<>(subPredicates);
    }

    @Override
    public String toString() {
        switch (type) {
            case EQUALS:
                return column + " = " + value;
            case NOT_EQUALS:
                return column + " != " + value;
            case LESS_THAN:
                return column + " < " + value;
            case LESS_THAN_EQUALS:
                return column + " <= " + value;
            case GREATER_THAN:
                return column + " > " + value;
            case GREATER_THAN_EQUALS:
                return column + " >= " + value;
            case IS_NULL:
                return column + " IS NULL";
            case IS_NOT_NULL:
                return column + " IS NOT NULL";
            case AND:
                return String.join(" AND ", subPredicates.stream().map(Object::toString).toList());
            case OR:
                return String.join(" OR ", subPredicates.stream().map(Object::toString).toList());
            case NOT:
                return "NOT (" + subPredicates.get(0) + ")";
            default:
                throw new IllegalStateException("Unexpected predicate type: " + type);
        }
    }

    /**
     * Types of predicates that can appear in a query plan.
     */
    public enum PredicateType {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        LESS_THAN_EQUALS,
        GREATER_THAN,
        GREATER_THAN_EQUALS,
        IS_NULL,
        IS_NOT_NULL,
        AND,
        OR,
        NOT
    }
} 