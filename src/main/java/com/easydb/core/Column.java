package com.easydb.core;

public record Column(String name, 
DataType type, boolean nullable, boolean primaryKey, boolean unique, Object defaultValue, int position) {
    public Column {
        if (name == null || type == null) {
            throw new IllegalStateException("Column name and type are required");
        }
    }

    public String toSqlDefinition() {
        StringBuilder sql = new StringBuilder();
        sql.append(name).append(" ").append(type.getSqlType());
        if (!nullable) sql.append(" NOT NULL");
        if (primaryKey) sql.append(" PRIMARY KEY");
        if (unique && !primaryKey) sql.append(" UNIQUE");
        if (defaultValue != null) {
            sql.append(" DEFAULT ");
            if (type.isText()) {
                sql.append("'").append(defaultValue).append("'");
            } else {
                sql.append(defaultValue);
            }
        }
        return sql.toString();
    }
}