package com.easydb.sql.planner.expression;

/**
 * Defines types of expressions in SQL queries.
 * Similar to PostgreSQL's NodeTag enum for expression nodes.
 */
public enum ExpressionType {
    // Basic expressions
    COLUMN_REF,     // Column reference (e.g., "table.column", "column")
    CONSTANT,       // Literal value (e.g., 42, 'text', true)
    
    // Complex expressions
    FUNCTION_CALL,  // Function invocation (e.g., COUNT(*), UPPER(name))
    ARITHMETIC,     // Arithmetic operations (e.g., price * quantity)
    
    // Logical expressions
    AND,           // Logical AND
    OR,            // Logical OR
    NOT,           // Logical NOT
    
    // Comparison expressions
    EQUALS,        // Equal (=)
    NOT_EQUALS,    // Not Equal (<>)
    LESS_THAN,     // Less Than (<)
    GREATER_THAN,  // Greater Than (>)
    LESS_EQUAL,    // Less Than or Equal (<=)
    GREATER_EQUAL, // Greater Than or Equal (>=)
    IS_NULL,       // IS NULL check
    LIKE,          // LIKE pattern matching
    IN,            // IN list check
    BETWEEN;       // BETWEEN range check

    /**
     * Check if this expression type can be used in a WHERE clause
     */
    public boolean isPredicateType() {
        return switch (this) {
            case AND, OR, NOT, EQUALS, NOT_EQUALS, 
                 LESS_THAN, GREATER_THAN, LESS_EQUAL, 
                 GREATER_EQUAL, IS_NULL, LIKE, IN, BETWEEN -> true;
            default -> false;
        };
    }

    /**
     * Check if this expression type returns a boolean result
     */
    public boolean isLogicalType() {
        return switch (this) {
            case AND, OR, NOT -> true;
            default -> false;
        };
    }

    /**
     * Check if this expression type is a comparison
     */
    public boolean isComparisonType() {
        return switch (this) {
            case EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, 
                 LESS_EQUAL, GREATER_EQUAL -> true;
            default -> false;
        };
    }
} 