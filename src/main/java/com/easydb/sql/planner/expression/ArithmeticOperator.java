package com.easydb.sql.planner.expression;

/**
 * Represents arithmetic operators in expressions.
 * Similar to PostgreSQL's operator definitions in nodes/primnodes.h
 */
public enum ArithmeticOperator {
    PLUS("+"),
    MINUS("-"),
    MULTIPLY("*"),
    DIVIDE("/"),
    MODULO("%");

    private final String symbol;

    ArithmeticOperator(String symbol) {
        this.symbol = symbol;
    }

    /**
     * Convert string operator to enum
     */
    public static ArithmeticOperator fromString(String symbol) {
        return switch (symbol) {
            case "+" -> PLUS;
            case "-" -> MINUS;
            case "*" -> MULTIPLY;
            case "/" -> DIVIDE;
            case "%" -> MODULO;
            default -> throw new IllegalArgumentException("Unknown operator: " + symbol);
        };
    }

    /**
     * Check if operator is commutative (a op b = b op a)
     */
    public boolean isCommutative() {
        return switch (this) {
            case PLUS, MULTIPLY -> true;
            case MINUS, DIVIDE, MODULO -> false;
        };
    }

    /**
     * Get operator precedence (higher means higher precedence)
     */
    public int getPrecedence() {
        return switch (this) {
            case MULTIPLY, DIVIDE, MODULO -> 2;
            case PLUS, MINUS -> 1;
        };
    }

    @Override
    public String toString() {
        return symbol;
    }
} 