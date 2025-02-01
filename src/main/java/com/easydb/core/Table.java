package com.easydb.core;

import java.util.*;

/**
 * Represents a database table schema.
 */
public class Table {
    private final String name;
    private final List<Column> columns;
    private final Map<String, Column> columnMap;
    private final List<ForeignKey> foreignKeys;

    public Table(String name, List<Column> columns, List<ForeignKey> foreignKeys) {
        this.name = name;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.columnMap = new HashMap<>();
        for (Column column : columns) {
            columnMap.put(column.name().toLowerCase(), column);
        }
        this.foreignKeys = Collections.unmodifiableList(new ArrayList<>(foreignKeys));
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Optional<Column> getColumn(String name) {
        return Optional.ofNullable(columnMap.get(name.toLowerCase()));
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public String toCreateTableSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(name).append(" (\n");
        
        // Add columns
        for (int i = 0; i < columns.size(); i++) {
            sql.append("    ").append(columns.get(i).toSqlDefinition());
            if (i < columns.size() - 1 || !foreignKeys.isEmpty()) {
                sql.append(",");
            }
            sql.append("\n");
        }

        // Add foreign keys
        for (int i = 0; i < foreignKeys.size(); i++) {
            sql.append("    ").append(foreignKeys.get(i).toSqlDefinition());
            if (i < foreignKeys.size() - 1) {
                sql.append(",");
            }
            sql.append("\n");
        }

        sql.append(")");
        return sql.toString();
    }

    public static class Builder {
        private final String name;
        private final List<Column> columns = new ArrayList<>();
        private final List<ForeignKey> foreignKeys = new ArrayList<>();

        public Builder(String name) {
            this.name = name;
        }

        public Builder addColumn(Column column) {
            columns.add(column);
            return this;
        }

        public Builder addForeignKey(ForeignKey foreignKey) {
            foreignKeys.add(foreignKey);
            return this;
        }

        public Table build() {
            return new Table(name, columns, foreignKeys);
        }
    }

} 