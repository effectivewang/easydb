package com.easydb.sql.result;

import com.easydb.core.Column;
import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.storage.metadata.TableMetadata;


import java.util.*;

/**
 * Represents a result set from a SQL query.
 */
public class ResultSet {
    private final List<Column> columns;
    private final List<Row> rows;

    private ResultSet(List<Column> columns, List<Row> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    public List<Row> getRows() {
        return Collections.unmodifiableList(rows);
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public int getRowCount() {
        return rows.size();
    }

    public static ResultSet empty() {
        return new Builder().build();
    }

    @Override
    public String toString() {
        return "ResultSet{" +
                "columns=" + Arrays.toString(columns.toArray()) +
                ", rows=" + Arrays.toString(rows.toArray()) +
                '}';
    }

    /**
     * Represents a row in the result set.
     */
    public static class Row {
        private final Map<String, Object> values;

        private Row(Map<String, Object> values) {
            this.values = values;
        }

        public String getString(String columnName) {
            Object value = values.get(columnName);
            return value != null ? value.toString() : null;
        }

        public Integer getInteger(String columnName) {
            Object value = values.get(columnName);
            if (value == null) return null;
            if (value instanceof Integer) return (Integer) value;
            if (value instanceof Long) return ((Long) value).intValue();
            return Integer.parseInt(value.toString());
        }

        public Boolean getBoolean(String columnName) {
            Object value = values.get(columnName);
            if (value == null) return null;
            if (value instanceof Boolean) return (Boolean) value;
            return Boolean.parseBoolean(value.toString());
        }

        public Object getValue(String columnName) {
            return values.get(columnName);
        }

        @Override
        public String toString() {
            return "Row{" +
                    "values=" + values +
                    '}';
        }
    }

    /**
     * Builder for creating ResultSet instances.
     */
    public static class Builder {
        private final List<Column> columns = new ArrayList<>();
        private final List<Row> rows = new ArrayList<>();

        public Builder addColumn(Column column) {
            columns.add(column);
            return this;
        }

        public Builder addRow(Map<String, Object> values) {
            rows.add(new Row(new HashMap<>(values)));
            return this;
        }

        public ResultSet build() {
            return new ResultSet(columns, rows);
        }

        public ResultSet build(List<Tuple> tuples) {
            tuples.forEach(tuple -> {
                List<String> columnNames = tuple.getColumnNames();
                
                Map<String, Object> values = new HashMap<>();
                List<Object> tupleValues = tuple.getValues();
                for (int i = 0; i < columnNames.size(); i++) {
                    values.put(columnNames.get(i), tupleValues.get(i));
                }
                addRow(values);
            });
            return build();
        }
    }
} 