package com.easydb.core;

/**
 * Represents a column in a database table.
 */
public class Column {
    private final String name;
    private final DataType type;
    private final boolean nullable;
    private final boolean primaryKey;
    private final boolean autoIncrement;
    private final Object defaultValue;
    private final int ordinalPosition;

    public Column(String name, DataType type, boolean nullable, boolean primaryKey, 
                 boolean autoIncrement, Object defaultValue, int ordinalPosition) {
        if (name == null || type == null) {
            throw new IllegalArgumentException("Column name and type are required");
        }
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.primaryKey = primaryKey;
        this.autoIncrement = autoIncrement;
        this.defaultValue = defaultValue;
        this.ordinalPosition = ordinalPosition;
    }

    public String name() {
        return name;
    }

    public DataType type() {
        return type;
    }

    public boolean nullable() {
        return nullable;
    }

    public boolean primaryKey() {
        return primaryKey;
    }

    public boolean autoIncrement() {
        return autoIncrement;
    }

    public Object defaultValue() {
        return defaultValue;
    }

    public int ordinalPosition() {
        return ordinalPosition;
    }

    public String toSqlDefinition() {
        StringBuilder sql = new StringBuilder();
        sql.append(name).append(" ").append(type.name());
        if (!nullable) sql.append(" NOT NULL");
        if (primaryKey) sql.append(" PRIMARY KEY");
        if (autoIncrement) sql.append(" AUTO_INCREMENT");
        if (defaultValue != null) {
            sql.append(" DEFAULT ");
            if (type == DataType.STRING) {
                sql.append("'").append(defaultValue).append("'");
            } else {
                sql.append(defaultValue);
            }
        }
        return sql.toString();
    }
}