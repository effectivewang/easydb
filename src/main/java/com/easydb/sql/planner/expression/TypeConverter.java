package com.easydb.sql.planner.expression;

import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import com.easydb.core.DataType;

/**
 * Unified type conversion system for SQL expressions.
 * Similar to PostgreSQL's type conversion system.
 */
public class TypeConverter {
    
    /**
     * Converts a ParseTree value to the appropriate Java type.
     */
    public static Object convertValue(ParseTree value) {
        if (value == null) {
            return null;
        }

        switch (value.getType()) {
            case INTEGER_TYPE:
                return Integer.parseInt(value.getValue());
            case DOUBLE_TYPE:
                return Double.parseDouble(value.getValue());
            case BOOLEAN_TYPE:
                return Boolean.parseBoolean(value.getValue());
            case STRING_TYPE:
                return value.getValue();
            case NULL_TYPE:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported value type: " + value.getType());
        }
    }

    /**
     * Converts a value to the specified DataType.
     */
    public static Object convertToDataType(Object value, DataType targetType) {
        if (value == null) {
            return null;
        }

        switch (targetType) {
            case INTEGER:
                if (value instanceof Integer) return value;
                if (value instanceof Long) return ((Long) value).intValue();
                if (value instanceof Double) return ((Double) value).intValue();
                return Integer.parseInt(value.toString());
                
            case DOUBLE:
                if (value instanceof Double) return value;
                if (value instanceof Integer) return ((Integer) value).doubleValue();
                if (value instanceof Long) return ((Long) value).doubleValue();
                return Double.parseDouble(value.toString());
                
            case STRING:
                return value.toString();
                
            case BOOLEAN:
                if (value instanceof Boolean) return value;
                return Boolean.parseBoolean(value.toString());
                
            default:
                throw new IllegalArgumentException("Unsupported target type: " + targetType);
        }
    }

    /**
     * Determines the common type for binary operations.
     * Similar to PostgreSQL's type resolution rules.
     */
    public static DataType resolveCommonType(DataType left, DataType right) {
        // If types are the same, return that type
        if (left == right) {
            return left;
        }

        // Handle numeric type promotion
        if (left == DataType.INTEGER && right == DataType.DOUBLE) {
            return DataType.DOUBLE;
        }
        if (left == DataType.DOUBLE && right == DataType.INTEGER) {
            return DataType.DOUBLE;
        }

        // Handle string concatenation
        if (left == DataType.STRING || right == DataType.STRING) {
            return DataType.STRING;
        }

        // Default to string if types are incompatible
        return DataType.STRING;
    }

    /**
     * Checks if a value can be converted to the specified type.
     */
    public static boolean canConvertTo(Object value, DataType targetType) {
        if (value == null) {
            return true; // NULL can be converted to any type
        }

        try {
            convertToDataType(value, targetType);
            return true;
        } catch (NumberFormatException | IllegalArgumentException e) {
            return false;
        }
    }
} 