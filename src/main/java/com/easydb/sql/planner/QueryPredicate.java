package com.easydb.sql.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents predicates for different types of SQL operations.
 * Supports SELECT, INSERT, UPDATE, and DELETE operations.
 */
public class QueryPredicate {
    private final PredicateType type;
    private final String tableName;
    private final List<String> columns;
    private final List<List<Object>> valuesList;
    private final List<QueryPredicate> subPredicates;

    // Constructor for comparison predicates (WHERE clause)
    private QueryPredicate(PredicateType type, String column, Object value, List<QueryPredicate> subPredicates) {
        this.type = type;
        this.tableName = null;
        this.columns = column != null ? List.of(column) : new ArrayList<>();
        this.valuesList = Arrays.asList(Arrays.asList(value));
        this.subPredicates = subPredicates != null ? new ArrayList<>(subPredicates) : new ArrayList<>();
    }

    // Constructor for DML operations (INSERT, UPDATE, DELETE)
    public QueryPredicate(String tableName, List<String> columns, List<Object> valuesList, List<QueryPredicate> subPredicates) {
        this.type = PredicateType.DML;
        this.tableName = tableName;
        this.columns = new ArrayList<>(columns);
        this.valuesList = Arrays.asList(valuesList);
        this.subPredicates = subPredicates;
    }

    // Factory methods for comparison predicates
    public static QueryPredicate equals(String column, Object value) {
        return new QueryPredicate(PredicateType.EQUALS, column, value, null);
    }

    public static QueryPredicate notEquals(String column, Object value) {
        return new QueryPredicate(PredicateType.NOT_EQUALS, column, value, null);
    }

    public static QueryPredicate lessThan(String column, Object value) {
        return new QueryPredicate(PredicateType.LESS_THAN, column, value, null);
    }

    public static QueryPredicate lessThanOrEquals(String column, Object value) {
        return new QueryPredicate(PredicateType.LESS_THAN_OR_EQUALS, column, value, null);
    }

    public static QueryPredicate greaterThan(String column, Object value) {
        return new QueryPredicate(PredicateType.GREATER_THAN, column, value, null);
    }

    public static QueryPredicate greaterThanOrEquals(String column, Object value) {
        return new QueryPredicate(PredicateType.GREATER_THAN_OR_EQUALS, column, value, null);
    }

    // Factory methods for logical operations
    public static QueryPredicate and(List<QueryPredicate> predicates) {
        return new QueryPredicate(PredicateType.AND, null, null, predicates);
    }

    public static QueryPredicate or(List<QueryPredicate> predicates) {
        return new QueryPredicate(PredicateType.OR, null, null, predicates);
    }

    public static QueryPredicate not(QueryPredicate predicate) {
        return new QueryPredicate(PredicateType.NOT, null, null, List.of(predicate));
    }

    public static QueryPredicate isNull(String columnName) {
        return new QueryPredicate(PredicateType.IS_NULL, columnName, null, null);
    }

    // Factory methods for DML operations
    public static QueryPredicate insert(String tableName, List<String> columns, List<Object> values) {
        return new QueryPredicate(tableName, columns, values, null);
    }

    public static QueryPredicate update(String tableName, List<String> columns, List<Object> values, List<QueryPredicate> whereConditions) {
        return new QueryPredicate(tableName, columns, values, whereConditions);
    }

    public static QueryPredicate delete(String tableName, List<QueryPredicate> whereConditions) {
        return new QueryPredicate(tableName, new ArrayList<>(), null, whereConditions);
    }

    // Getters
    public PredicateType getType() {
        return type;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    public List<List<Object>> getValues() {
        return new ArrayList<>(valuesList);
    }

    public List<QueryPredicate> getSubPredicates() {
        return new ArrayList<>(subPredicates);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        if (type == PredicateType.DML) {
            sb.append("table=").append(tableName);
            if (!columns.isEmpty()) {
                sb.append(", columns=").append(columns);
            }
            if (valuesList != null) {
                sb.append(", values=").append(valuesList);
            }
        } else {
            switch (type) {
                case EQUALS:
                    sb.append(columns.get(0)).append(" = ").append(valuesList.get(0));
                    break;
                case NOT_EQUALS:
                    sb.append(columns.get(0)).append(" != ").append(valuesList.get(0));
                    break;
                case LESS_THAN:
                    sb.append(columns.get(0)).append(" < ").append(valuesList.get(0));
                    break;
                case GREATER_THAN:
                    sb.append(columns.get(0)).append(" > ").append(valuesList.get(0));
                    break;
                case AND:
                    sb.append("AND ").append(subPredicates);
                    break;
                case OR:
                    sb.append("OR ").append(subPredicates);
                    break;
                case NOT:
                    sb.append("NOT ").append(subPredicates.get(0));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
        
        return sb.toString();
    }

    /**
     * Types of predicates that can appear in a query plan.
     */
    public enum PredicateType {
        // Comparison operators
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        LESS_THAN_OR_EQUALS,
        GREATER_THAN,
        GREATER_THAN_OR_EQUALS,
        IS_NULL,
        
        // Logical operators
        AND,
        OR,
        NOT,
        
        // DML operations
        DML
    }
} 