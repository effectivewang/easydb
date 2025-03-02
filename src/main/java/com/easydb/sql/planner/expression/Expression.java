package com.easydb.sql.planner.expression;

import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import com.easydb.sql.planner.RangeTableEntry;

/**
 * Represents an expression in a query, similar to PostgreSQL's Expr node.
 */
public class Expression {
    private final ExpressionType type;
    private final String value;          // Column name, function name, or constant value
    private final Expression left;       // Left operand for arithmetic/logical
    private final Expression right;      // Right operand for arithmetic/logical
    private final List<Expression> arguments;  // Function arguments
    private final ArithmeticOperator operator; // Arithmetic operator

    // Constructor for column references and constants
    public Expression(ExpressionType type, String value) {
        this.type = type;
        this.value = value;
        this.left = null;
        this.right = null;
        this.arguments = new ArrayList<>();
        this.operator = null;
    }

    // Constructor for function calls
    public Expression(ExpressionType type, String functionName, List<Expression> arguments) {
        this.type = type;
        this.value = functionName;
        this.left = null;
        this.right = null;
        this.arguments = arguments;
        this.operator = null;
    }

    // Constructor for arithmetic expressions
    public Expression(ExpressionType type, String operatorStr, Expression left, Expression right) {
        this.type = type;
        this.value = null;
        this.left = left;
        this.right = right;
        this.arguments = new ArrayList<>();
        this.operator = operatorStr == null ? null : ArithmeticOperator.fromString(operatorStr);
    }

    public ExpressionType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    public List<Expression> getArguments() {
        return arguments;
    }

    public String getFunctionName() {
        if (type != ExpressionType.FUNCTION_CALL) {
            throw new IllegalStateException("Not a function call expression");
        }
        return value;
    }

    public ArithmeticOperator getOperator() {
        if (type != ExpressionType.ARITHMETIC) {
            throw new IllegalStateException("Not an arithmetic expression");
        }
        return operator;
    }

    @Override
    public String toString() {
        return switch (type) {
            case COLUMN_REF -> value;
            case CONSTANT -> value instanceof String ? "'" + value + "'" : value;
            case FUNCTION_CALL -> {
                StringBuilder sb = new StringBuilder(value).append('(');
                for (int i = 0; i < arguments.size(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(arguments.get(i));
                }
                yield sb.append(')').toString();
            }
            case ARITHMETIC -> "(" + left + " " + operator + " " + right + ")";
            case AND -> "(" + left + " AND " + right + ")";
            case OR -> "(" + left + " OR " + right + ")";
            case NOT -> "NOT (" + value + ")";
            case EQUALS -> "(" + left + " = " + right + ")";
            case NOT_EQUALS -> "(" + left + " <> " + right + ")";
            case LESS_THAN -> "(" + left + " < " + right + ")";
            case LESS_EQUAL -> "(" + left + " <= " + right + ")";
            case GREATER_THAN -> "(" + left + " > " + right + ")";
            case GREATER_EQUAL -> "(" + left + " >= " + right + ")";
            default -> throw new IllegalStateException("Unknown expression type: " + type);
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Expression)) return false;
        Expression that = (Expression) o;
        return type == that.type &&
               Objects.equals(value, that.value) &&
               Objects.equals(left, that.left) &&
               Objects.equals(right, that.right) &&
               Objects.equals(arguments, that.arguments) &&
               operator == that.operator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value, left, right, arguments, operator);
    }

    // Factory methods for better readability
    public static Expression column(String columnName) {
        return new Expression(ExpressionType.COLUMN_REF, columnName);
    }

    public static Expression constant(Object value) {
        return new Expression(ExpressionType.CONSTANT, String.valueOf(value));
    }

    public static Expression arithmetic(String operator, Expression left, Expression right) {
        return new Expression(ExpressionType.ARITHMETIC, operator, left, right);
    }

    public static Expression comparison(ExpressionType type, Expression left, Expression right) {
        return new Expression(type, null, left, right);
    }

    public static Expression and(Expression left, Expression right) {
        return new Expression(ExpressionType.AND, null, left, right);
    }

    public static Expression or(Expression left, Expression right) {
        return new Expression(ExpressionType.OR, null, left, right);
    }

    public static Expression not(Expression operand) {
        return new Expression(ExpressionType.NOT, null, operand, null);
    }

}
