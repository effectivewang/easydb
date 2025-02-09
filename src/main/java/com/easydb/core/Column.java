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

    public Column(String name, DataType type) {
        this(name, type, true, false, false, null);
    }

    public Column(String name, DataType type, boolean nullable, boolean primaryKey, 
                 boolean autoIncrement, Object defaultValue) {
        if (name == null || type == null) {
            throw new IllegalArgumentException("Column name and type are required");
        }
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.primaryKey = primaryKey;
        this.autoIncrement = autoIncrement;
        this.defaultValue = defaultValue;
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

    public Object parseValue(String value) {
        if (value.equalsIgnoreCase("null")) {
            if (defaultValue != null) {
                return defaultValue;
            } else {
                if (type == DataType.STRING) {
                    return "";
                } else if (type == DataType.INTEGER) {
                    return 0;
                } else {
                    throw new IllegalArgumentException("Invalid default value for column " + name);
                }
            }
        } else if (type == DataType.STRING) {
            return value;
        } else {
            return Integer.parseInt(value);
        }
    }
}