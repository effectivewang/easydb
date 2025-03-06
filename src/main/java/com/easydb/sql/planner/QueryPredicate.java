package com.easydb.sql.planner;

import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Objects;

/**
 * Represents predicates for SQL query conditions.
 * Similar to PostgreSQL's Expr structure.
 */
public class QueryPredicate {
    private final PredicateType type;
    private final String column;
    private final Object value;
    private final QueryPredicate left;
    private final QueryPredicate right;

    // Constructor for comparison predicates
    public QueryPredicate(PredicateType type, String column, Object value) {
        this.type = type;
        this.column = column;
        this.value = value;
        this.left = null;
        this.right = null;
    }

    // Constructor for logical operations (AND, OR, NOT)
    public QueryPredicate(PredicateType type, QueryPredicate left, QueryPredicate right) {
        this.type = type;
        this.column = null;
        this.value = null;
        this.left = left;
        this.right = right;
    }

    public PredicateType getType() {
        return type;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    public QueryPredicate getLeft() {
        return left;
    }

    public QueryPredicate getRight() {
        return right;
    }

    // Factory methods for creating predicates
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

    public static QueryPredicate and(QueryPredicate left, QueryPredicate right) {
        return new QueryPredicate(PredicateType.AND, left, right);
    }

    public static QueryPredicate or(QueryPredicate left, QueryPredicate right) {
        return new QueryPredicate(PredicateType.OR, left, right);
    }

    public static QueryPredicate not(QueryPredicate predicate) {
        return new QueryPredicate(PredicateType.NOT, predicate, null);
    }

    public enum PredicateType {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        GREATER_THAN,
        LESS_THAN_OR_EQUALS,
        GREATER_THAN_OR_EQUALS,
        IS_NULL,
        AND,
        OR,
        NOT
    }

    @Override
    public String toString() {
        if (type == PredicateType.AND || type == PredicateType.OR) {
            return String.format("(%s %s %s)", left, type, right);
        } else if (type == PredicateType.NOT) {
            return String.format("NOT %s", left);
        } else {
            return String.format("%s %s %s", column, type, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryPredicate)) return false;
        QueryPredicate that = (QueryPredicate) o;
        return type == that.type &&
               Objects.equals(column, that.column) &&
               Objects.equals(value, that.value) &&
               Objects.equals(left, that.left) &&
               Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, column, value, left, right);
    }
} 
