package com.easydb.sql.planner.expression;

import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import com.easydb.sql.planner.RangeTableEntry;

/**
 * Unified expression model for SQL queries, similar to PostgreSQL's Expr structure.
 * Handles both boolean predicates and general expressions.
 */
public class Expression {
    private final ExpressionType type;
    private final Object value;          // Column name, function name, or constant value
    private final Expression left;       // Left operand for arithmetic/logical
    private final Expression right;      // Right operand for arithmetic/logical
    private final List<Expression> arguments;  // Function arguments
    private final Operator operator;     // Operator for arithmetic/comparison/logical

    // Constructor for column references and constants
    public Expression(ExpressionType type, Object value) {
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

    // Constructor for operators (arithmetic, comparison, logical)
    public Expression(ExpressionType type, Operator operator, Expression left, Expression right) {
        this.type = type;
        this.value = null;
        this.left = left;
        this.right = right;
        this.arguments = new ArrayList<>();
        this.operator = operator;
    }

    public ExpressionType getType() {
        return type;
    }

    public Object getValue() {
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
        return (String) value;
    }

    public Operator getOperator() {
        if (type != ExpressionType.ARITHMETIC && 
            type != ExpressionType.COMPARISON && 
            type != ExpressionType.LOGICAL) {
            throw new IllegalStateException("Not an operator expression");
        }
        return operator;
    }

    @Override
    public String toString() {
        return switch (type) {
            case COLUMN_REF -> value.toString();
            case CONSTANT -> value.toString();
            case FUNCTION_CALL -> {
                StringBuilder sb = new StringBuilder(value.toString()).append('(');
                for (int i = 0; i < arguments.size(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(arguments.get(i));
                }
                yield sb.append(')').toString();
            }
            case ARITHMETIC -> "(" + left + " " + operator + " " + right + ")";
            case COMPARISON -> "(" + left + " " + operator + " " + right + ")";
            case LOGICAL -> "(" + left + " " + operator + " " + right + ")";
            case NOT -> "NOT (" + left + ")";
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
        return new Expression(ExpressionType.CONSTANT, value);
    }

    public static Expression arithmetic(Operator operator, Expression left, Expression right) {
        return new Expression(ExpressionType.ARITHMETIC, operator, left, right);
    }

    public static Expression comparison(Operator operator, Expression left, Expression right) {
        return new Expression(ExpressionType.COMPARISON, operator, left, right);
    }

    public static Expression logical(Operator operator, Expression left, Expression right) {
        return new Expression(ExpressionType.LOGICAL, operator, left, right);
    }

    public static Expression not(Expression operand) {
        return new Expression(ExpressionType.NOT, null, operand, null);
    }

    public static Expression function(String name, List<Expression> arguments) {
        return new Expression(ExpressionType.FUNCTION_CALL, name, arguments);
    }

    public enum ExpressionType {
        COLUMN_REF,
        CONSTANT,
        FUNCTION_CALL,
        ARITHMETIC,
        COMPARISON,
        LOGICAL,
        NOT
    }

    public enum Operator {
        // Arithmetic operators
        PLUS("+"),
        MINUS("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        
        // Comparison operators
        EQUALS("="),
        NOT_EQUALS("<>"),
        LESS_THAN("<"),
        LESS_EQUAL("<="),
        GREATER_THAN(">"),
        GREATER_EQUAL(">="),
        
        // Logical operators
        AND("AND"),
        OR("OR");

        private final String symbol;

        Operator(String symbol) {
            this.symbol = symbol;
        }

        @Override
        public String toString() {
            return symbol;
        }

        public static Operator fromString(String symbol) {
            for (Operator op : values()) {
                if (op.symbol.equals(symbol)) {
                    return op;
                }
            }
            throw new IllegalArgumentException("Unknown operator: " + symbol);
        }
    }
}
