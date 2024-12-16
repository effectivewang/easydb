package com.easydb.core;

/**
 * Represents a column in a database table.
 */
public record Column(
    String name,
    DataType type,
    boolean nullable,
    boolean primaryKey,
    boolean unique,
    Object defaultValue,
    int position
) {
    public static class Builder {
        private String name;
        private DataType type;
        private boolean nullable = true;
        private boolean primaryKey = false;
        private boolean unique = false;
        private Object defaultValue = null;
        private int position;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder type(DataType type) {
            this.type = type;
            return this;
        }

        public Builder nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder primaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
            if (primaryKey) {
                this.nullable = false;
            }
            return this;
        }

        public Builder unique(boolean unique) {
            this.unique = unique;
            return this;
        }

        public Builder defaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder position(int position) {
            this.position = position;
            return this;
        }

        public Column build() {
            if (name == null || type == null) {
                throw new IllegalStateException("Column name and type are required");
            }
            return new Column(name, type, nullable, primaryKey, unique, defaultValue, position);
        }
    }

    public String toSqlDefinition() {
        StringBuilder sql = new StringBuilder();
        sql.append(name).append(" ").append(type.getSqlType());
        
        if (!nullable) {
            sql.append(" NOT NULL");
        }
        if (primaryKey) {
            sql.append(" PRIMARY KEY");
        }
        if (unique && !primaryKey) {
            sql.append(" UNIQUE");
        }
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