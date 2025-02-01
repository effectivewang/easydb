package com.easydb.core;

import java.util.List;
import java.util.ArrayList;

/**
 * Represents a foreign key constraint.
 */
public class ForeignKey {
    private String name;
    private List<String> columns;
    private String referenceTable;
    private List<String> referenceColumns;
    private OnDelete onDelete;
    private OnUpdate onUpdate;
    
    public ForeignKey(String name, List<String> columns, String referenceTable, List<String> referenceColumns, OnDelete onDelete, OnUpdate onUpdate) {
        this.name = name;
        this.columns = columns;
        this.referenceTable = referenceTable;
        this.referenceColumns = referenceColumns;
        this.onDelete = onDelete;
        this.onUpdate = onUpdate;
    }

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