package com.easydb.sql.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Objects;

/**
 * Represents predicates for SQL query conditions.
 * Simplified to handle single column-value comparisons and logical operations.
 */
public class QueryPredicate implements Operation {
    private final PredicateType type;
    private final String column;           // Single column name
    private final Object value;           // Single value
    private final List<QueryPredicate> subPredicates;

    // Constructor for comparison predicates
    private QueryPredicate(PredicateType type, String column, Object value) {
        this.type = type;
        this.column = column;
        this.value = value;
        this.subPredicates = new ArrayList<>();
    }

    // Constructor for logical operations (AND, OR, NOT)
    private QueryPredicate(PredicateType type, List<QueryPredicate> predicates) {
        this.type = type;
        this.column = null;
        this.value = null;
        this.subPredicates = new ArrayList<>(predicates);
    }

    // Factory methods for comparison predicates
    public static QueryPredicate equals(String column, Object value) {
        return new QueryPredicate(PredicateType.EQUALS, column, value);
    }

    public static QueryPredicate notEquals(String column, Object value) {
        return new QueryPredicate(PredicateType.NOT_EQUALS, column, value);
    }

    public static QueryPredicate lessThan(String column, Object value) {
        return new QueryPredicate(PredicateType.LESS_THAN, column, value);
    }

    public static QueryPredicate greaterThan(String column, Object value) {
        return new QueryPredicate(PredicateType.GREATER_THAN, column, value);
    }

    public static QueryPredicate lessThanOrEquals(String column, Object value) {
        return new QueryPredicate(PredicateType.LESS_THAN_OR_EQUALS, column, value);
    }

    public static QueryPredicate greaterThanOrEquals(String column, Object value) {
        return new QueryPredicate(PredicateType.GREATER_THAN_OR_EQUALS, column, value);
    }

    public static QueryPredicate isNull(String column) {
        return new QueryPredicate(PredicateType.IS_NULL, column, null);
    }

    // Factory methods for logical operations
    public static QueryPredicate and(List<QueryPredicate> predicates) {
        return new QueryPredicate(PredicateType.AND, predicates);
    }

    public static QueryPredicate or(List<QueryPredicate> predicates) {
        return new QueryPredicate(PredicateType.OR, predicates);
    }

    public static QueryPredicate not(QueryPredicate predicate) {
        return new QueryPredicate(PredicateType.NOT, List.of(predicate));
    }

    // Getters
    public PredicateType getPredicateType() {
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

    public PredicateType getType() {
        return type;
    }

    @Override
    public QueryOperator getOperator() {
        return QueryOperator.INSERT;
    }

    @Override
    public String toString() {
        return switch (type) {
            case EQUALS -> formatComparison("=");
            case NOT_EQUALS -> formatComparison("!=");
            case LESS_THAN -> formatComparison("<");
            case GREATER_THAN -> formatComparison(">");
            case LESS_THAN_OR_EQUALS -> formatComparison("<=");
            case GREATER_THAN_OR_EQUALS -> formatComparison(">=");
            case IS_NULL -> formatIsNull();
            case AND -> formatLogical(" AND ");
            case OR -> formatLogical(" OR ");
            case NOT -> formatNot();
        };
    }

    private String formatComparison(String operator) {
        String valueStr = formatValue(value);
        return String.format("%s %s %s", column, operator, valueStr);
    }

    private String formatIsNull() {
        return String.format("%s IS NULL", column);
    }

    private String formatLogical(String operator) {
        if (subPredicates.isEmpty()) {
            return "";
        }
        return subPredicates.stream()
                .map(Object::toString)
                .collect(Collectors.joining(operator));
    }

    private String formatNot() {
        if (subPredicates.isEmpty()) {
            return "NOT";
        }
        return "NOT (" + subPredicates.get(0).toString() + ")";
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        // Don't quote simple values in the output
        return value.toString();
    }

    public enum PredicateType {
        // Comparison operators
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        GREATER_THAN,
        LESS_THAN_OR_EQUALS,
        GREATER_THAN_OR_EQUALS,
        IS_NULL,
        
        // Logical operators
        AND,
        OR,
        NOT
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryPredicate)) return false;
        QueryPredicate that = (QueryPredicate) o;
        return type == that.type &&
               Objects.equals(column, that.column) &&
               Objects.equals(value, that.value) &&
               Objects.equals(subPredicates, that.subPredicates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, column, value, subPredicates);
    }

    
} 
