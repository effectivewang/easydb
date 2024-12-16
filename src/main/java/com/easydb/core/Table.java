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

    /**
     * Represents a foreign key constraint.
     */
    public record ForeignKey(
        String name,
        List<String> columns,
        String referenceTable,
        List<String> referenceColumns,
        OnDelete onDelete,
        OnUpdate onUpdate
    ) {
        public String toSqlDefinition() {
            StringBuilder sql = new StringBuilder();
            sql.append("CONSTRAINT ").append(name).append(" FOREIGN KEY (")
               .append(String.join(", ", columns))
               .append(") REFERENCES ").append(referenceTable)
               .append(" (").append(String.join(", ", referenceColumns)).append(")");

            if (onDelete != OnDelete.NO_ACTION) {
                sql.append(" ON DELETE ").append(onDelete.sql);
            }
            if (onUpdate != OnUpdate.NO_ACTION) {
                sql.append(" ON UPDATE ").append(onUpdate.sql);
            }

            return sql.toString();
        }

        public enum OnDelete {
            NO_ACTION("NO ACTION"),
            CASCADE("CASCADE"),
            SET_NULL("SET NULL"),
            SET_DEFAULT("SET DEFAULT");

            private final String sql;

            OnDelete(String sql) {
                this.sql = sql;
            }
        }

        public enum OnUpdate {
            NO_ACTION("NO ACTION"),
            CASCADE("CASCADE"),
            SET_NULL("SET NULL"),
            SET_DEFAULT("SET DEFAULT");

            private final String sql;

            OnUpdate(String sql) {
                this.sql = sql;
            }
        }

        public static class Builder {
            private String name;
            private List<String> columns = new ArrayList<>();
            private String referenceTable;
            private List<String> referenceColumns = new ArrayList<>();
            private OnDelete onDelete = OnDelete.NO_ACTION;
            private OnUpdate onUpdate = OnUpdate.NO_ACTION;

            public Builder name(String name) {
                this.name = name;
                return this;
            }

            public Builder addColumn(String column) {
                this.columns.add(column);
                return this;
            }

            public Builder referenceTable(String table) {
                this.referenceTable = table;
                return this;
            }

            public Builder addReferenceColumn(String column) {
                this.referenceColumns.add(column);
                return this;
            }

            public Builder onDelete(OnDelete onDelete) {
                this.onDelete = onDelete;
                return this;
            }

            public Builder onUpdate(OnUpdate onUpdate) {
                this.onUpdate = onUpdate;
                return this;
            }

            public ForeignKey build() {
                if (name == null || referenceTable == null || 
                    columns.isEmpty() || referenceColumns.isEmpty()) {
                    throw new IllegalStateException("Missing required foreign key properties");
                }
                return new ForeignKey(name, columns, referenceTable, 
                                    referenceColumns, onDelete, onUpdate);
            }
        }
    }
} 