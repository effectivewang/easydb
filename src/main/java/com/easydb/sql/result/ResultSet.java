package com.easydb.sql.result;

import com.easydb.core.DataType;
import com.easydb.core.Column;
import java.util.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;

/**
 * A structured result set for SQL query results.
 */
public class ResultSet implements Iterable<ResultSet.Row> {
    private final List<Column> columns;
    private final List<Row> rows;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE;
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    public ResultSet(List<Column> columns, List<Row> rows) {
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.rows = Collections.unmodifiableList(new ArrayList<>(rows));
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Row> getRows() {
        return rows;
    }

    public Optional<Column> getColumn(String name) {
        return columns.stream()
                .filter(col -> col.name().equalsIgnoreCase(name))
                .findFirst();
    }

    public int getColumnCount() {
        return columns.size();
    }

    public int getRowCount() {
        return rows.size();
    }

    @Override
    public Iterator<Row> iterator() {
        return rows.iterator();
    }

    /**
     * Convert entire ResultSet to CSV format.
     */
    public String toCsv() {
        StringBuilder csv = new StringBuilder();
        
        // Add header
        csv.append(columns.stream()
            .map(Column::name)
            .reduce((a, b) -> a + "," + b)
            .orElse(""))
            .append("\n");

        // Add rows
        for (Row row : rows) {
            csv.append(row.toCsv()).append("\n");
        }

        return csv.toString();
    }

    /**
     * Convert entire ResultSet to JSON array format.
     */
    public String toJson() {
        StringBuilder json = new StringBuilder();
        json.append("[\n");
        
        for (int i = 0; i < rows.size(); i++) {
            json.append("  ").append(rows.get(i).toJson());
            if (i < rows.size() - 1) {
                json.append(",");
            }
            json.append("\n");
        }
        
        json.append("]");
        return json.toString();
    }

    /**
     * Represents a row in the result set.
     */
    public class Row {
        private final Object[] values;

        public Row(Object[] values) {
            this.values = values;
        }

        public <T> T get(int index, Class<T> type) {
            if (index < 0 || index >= values.length) {
                throw new IndexOutOfBoundsException("Column index out of range: " + index);
            }
            return type.cast(values[index]);
        }

        public <T> T get(String columnName, Class<T> type) {
            Optional<Column> column = getColumn(columnName);
            if (column.isEmpty()) {
                throw new IllegalArgumentException("Column not found: " + columnName);
            }
            return get(column.get().position(), type);
        }

        public Object get(int index) {
            if (index < 0 || index >= values.length) {
                throw new IndexOutOfBoundsException("Column index out of range: " + index);
            }
            return values[index];
        }

        public Object get(String columnName) {
            Optional<Column> column = getColumn(columnName);
            if (column.isEmpty()) {
                throw new IllegalArgumentException("Column not found: " + columnName);
            }
            return get(column.get().position());
        }

        public boolean isNull(int index) {
            if (index < 0 || index >= values.length) {
                throw new IndexOutOfBoundsException("Column index out of range: " + index);
            }
            return values[index] == null;
        }

        public boolean isNull(String columnName) {
            Optional<Column> column = getColumn(columnName);
            if (column.isEmpty()) {
                throw new IllegalArgumentException("Column not found: " + columnName);
            }
            return isNull(column.get().position());
        }

        // Convenience methods for common types
        public String getString(String columnName) {
            return get(columnName, String.class);
        }

        public Integer getInteger(String columnName) {
            return get(columnName, Integer.class);
        }

        public Long getLong(String columnName) {
            return get(columnName, Long.class);
        }

        public Double getDouble(String columnName) {
            return get(columnName, Double.class);
        }

        public BigDecimal getDecimal(String columnName) {
            return get(columnName, BigDecimal.class);
        }

        public Boolean getBoolean(String columnName) {
            return get(columnName, Boolean.class);
        }

        public LocalDate getDate(String columnName) {
            return get(columnName, LocalDate.class);
        }

        public LocalDateTime getDateTime(String columnName) {
            return get(columnName, LocalDateTime.class);
        }

        public byte[] getBytes(String columnName) {
            return get(columnName, byte[].class);
        }

        /**
         * Convert row to JSON object format.
         */
        public String toJson() {
            StringBuilder json = new StringBuilder();
            json.append("{");
            
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                if (i > 0) {
                    json.append(", ");
                }
                json.append("\"").append(escapeJson(column.name())).append("\": ");
                json.append(formatValueAsJson(values[i], column.type()));
            }
            
            json.append("}");
            return json.toString();
        }

        /**
         * Convert row to CSV format.
         */
        public String toCsv() {
            StringBuilder csv = new StringBuilder();
            
            for (int i = 0; i < values.length; i++) {
                if (i > 0) {
                    csv.append(",");
                }
                csv.append(formatValueAsCsv(values[i], columns.get(i).type()));
            }
            
            return csv.toString();
        }

        private String formatValueAsJson(Object value, DataType type) {
            if (value == null) {
                return "null";
            }

            switch (type) {
                case STRING:
                    return "\"" + escapeJson(value.toString()) + "\"";
                case DATE:
                    return "\"" + ((LocalDate) value).format(DATE_FORMATTER) + "\"";
                case DATETIME:
                    return "\"" + ((LocalDateTime) value).format(DATETIME_FORMATTER) + "\"";
                case BYTES:
                    return "\"" + Base64.getEncoder().encodeToString((byte[]) value) + "\"";
                case BOOLEAN:
                case INTEGER:
                case LONG:
                case DOUBLE:
                case DECIMAL:
                    return value.toString();
                default:
                    throw new IllegalStateException("Unsupported type: " + type);
            }
        }

        private String formatValueAsCsv(Object value, DataType type) {
            if (value == null) {
                return "";
            }

            String formatted = switch (type) {
                case STRING -> escapeQuotes(value.toString());
                case DATE -> ((LocalDate) value).format(DATE_FORMATTER);
                case DATETIME -> ((LocalDateTime) value).format(DATETIME_FORMATTER);
                case BYTES -> Base64.getEncoder().encodeToString((byte[]) value);
                default -> value.toString();
            };

            // Quote values that contain commas or quotes
            if (formatted.contains(",") || formatted.contains("\"") || formatted.contains("\n")) {
                return "\"" + formatted + "\"";
            }
            return formatted;
        }

        private String escapeJson(String value) {
            return value.replace("\\", "\\\\")
                       .replace("\"", "\\\"")
                       .replace("\n", "\\n")
                       .replace("\r", "\\r")
                       .replace("\t", "\\t");
        }

        private String escapeQuotes(String value) {
            return value.replace("\"", "\"\"");
        }
    }

    /**
     * Builder for creating ResultSet instances.
     */
    public static class Builder {
        private final List<Column> columns = new ArrayList<>();
        private final List<Row> rows = new ArrayList<>();
        private final ResultSet resultSet;

        public Builder() {
            this.resultSet = new ResultSet(columns, rows);
        }

        public Builder addColumn(String name, DataType type, boolean nullable) {
            columns.add(new Column.Builder()
                .name(name)
                .type(type)
                .nullable(nullable)
                .position(columns.size())
                .build());
            return this;
        }

        public Builder addRow(Object... values) {
            if (values.length != columns.size()) {
                throw new IllegalArgumentException(
                    "Value count doesn't match column count. Expected: " + 
                    columns.size() + ", Got: " + values.length);
            }
            rows.add(resultSet.new Row(values));
            return this;
        }

        public ResultSet build() {
            return new ResultSet(columns, rows);
        }
    }
} 