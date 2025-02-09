package com.easydb.core;

/**
 * Represents supported data types in the database.
 */
public enum DataType {
    INTEGER(Integer.class),
    LONG(Long.class),
    DOUBLE(Double.class),
    STRING(String.class),
    BOOLEAN(Boolean.class),
    BYTES(byte[].class);

    private final Class<?> javaType;

    DataType(Class<?> javaType) {
        this.javaType = javaType;
    }

    public Class<?> getJavaType() {
        return javaType;
    }

    public static DataType parse(String typeLiteral) {
        return switch (typeLiteral.toUpperCase()) {
            case "INTEGER", "INT" -> DataType.INTEGER;
            case "STRING", "VARCHAR", "TEXT" -> DataType.STRING;
            case "BOOLEAN", "BOOL" -> DataType.BOOLEAN;
            case "DOUBLE", "FLOAT" -> DataType.DOUBLE;
            default -> throw new IllegalArgumentException("Unsupported data type: " + typeLiteral);
        };
    }
} 