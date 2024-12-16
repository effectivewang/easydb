package com.easydb.core;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Common data type system for EasyDB.
 */
public enum DataType {
    STRING(String.class, "VARCHAR"),
    INTEGER(Integer.class, "INTEGER"),
    LONG(Long.class, "BIGINT"),
    DOUBLE(Double.class, "DOUBLE"),
    DECIMAL(BigDecimal.class, "DECIMAL"),
    BOOLEAN(Boolean.class, "BOOLEAN"),
    DATE(LocalDate.class, "DATE"),
    DATETIME(LocalDateTime.class, "TIMESTAMP"),
    BYTES(byte[].class, "BLOB");

    private final Class<?> javaType;
    private final String sqlType;

    DataType(Class<?> javaType, String sqlType) {
        this.javaType = javaType;
        this.sqlType = sqlType;
    }

    public Class<?> getJavaType() {
        return javaType;
    }

    public String getSqlType() {
        return sqlType;
    }

    public static DataType fromJavaType(Class<?> type) {
        for (DataType dataType : values()) {
            if (dataType.javaType.equals(type)) {
                return dataType;
            }
        }
        throw new IllegalArgumentException("Unsupported Java type: " + type);
    }

    public static DataType fromSqlType(String sqlType) {
        for (DataType dataType : values()) {
            if (dataType.sqlType.equalsIgnoreCase(sqlType)) {
                return dataType;
            }
        }
        throw new IllegalArgumentException("Unsupported SQL type: " + sqlType);
    }

    public boolean isNumeric() {
        return this == INTEGER || this == LONG || this == DOUBLE || this == DECIMAL;
    }

    public boolean isTemporal() {
        return this == DATE || this == DATETIME;
    }

    public boolean isText() {
        return this == STRING;
    }

    public boolean isBinary() {
        return this == BYTES;
    }
} 