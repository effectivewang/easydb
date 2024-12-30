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
} 