package com.easydb.sql.executor;

import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.storage.TupleId;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.core.Column;
import com.easydb.core.DataType;
import com.easydb.sql.planner.operation.InsertOperation;
import com.easydb.storage.transaction.Transaction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Arrays;
/**
 * Dedicated executor for DML (Data Manipulation Language) operations.
 * Currently handles INSERT operations.
 */
public class DMLExecutor {
    private final Storage storage;
    private final ExecutionContext executionContext;

    public DMLExecutor(Storage storage, ExecutionContext executionContext) {
        this.storage = storage;
        this.executionContext = executionContext;
    }

    public List<Tuple> executeInsert(InsertOperation operation) {
        String tableName = operation.getTableName();
        List<String> columns = operation.getColumns();
        List<List<Object>> values = operation.getValues();
        
        // Get table metadata
        TableMetadata metadata = storage.getTableMetadata(tableName);
        if (metadata == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        // Validate column existence and types
        validateColumns(metadata, columns);

        List<Tuple> insertedTuples = new ArrayList<>();
        Transaction txn = executionContext.getTransaction();

        try {
            // Insert each row
            for (List<Object> rowValues : values) {
                // Create new tuple with the specified columns and values
                Tuple tuple = createTuple(metadata, columns, rowValues);
                
                // Validate constraints (NOT NULL, UNIQUE, etc.)
                validateConstraints(metadata, tuple);
                
                // Perform the actual insert
                storage.insertTuple(tuple);
                
                insertedTuples.add(tuple);
            }
            
            return insertedTuples;
        } catch (Exception e) {
            throw new RuntimeException("Insert failed: " + e.getMessage(), e);
        }
    }

    private void validateColumns(TableMetadata metadata, List<String> columns) {
        // Check if all columns exist in the table
        for (String column : columns) {
            if (!metadata.hasColumn(column)) {
                throw new IllegalArgumentException(
                    "Column not found in table " + metadata.tableName() + ": " + column);
            }
        }
        
        // Check if all required (NOT NULL) columns are included
        for (Column column : metadata.columns()) {
            if (!column.nullable() && !columns.contains(column.name())) {
                throw new IllegalArgumentException(
                    "Required column missing: " + column.name());
            }
        }
    }

    private Tuple createTuple(TableMetadata metadata, List<String> columns, List<Object> values) {
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException(
                "Column count does not match value count");
        }

        // Create a new tuple with default values
        List<Object> tupleValues = new ArrayList<>();
        // Set the provided values
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            Object value = values.get(i);
            
            // Validate and convert the value to the correct type
            Column column = metadata.getColumn(columnName);
            Object convertedValue = convertValue(value, column.type());
            tupleValues.add(convertedValue);
        }
        
        return new Tuple(new TupleId(metadata.tableName(), Instant.now().toEpochMilli()), tupleValues, metadata, 1);
    }

    private Object convertValue(Object value, DataType targetType) {
        if (value == null) {
            return null;
        }
        
        try {
            return switch (targetType) {
                case INTEGER -> value instanceof Integer ? value : Integer.valueOf(value.toString());
                case STRING -> value.toString();
                case BOOLEAN -> value instanceof Boolean ? value : Boolean.valueOf(value.toString());
                case DOUBLE -> value instanceof Double ? value : Double.valueOf(value.toString());
                case NULL -> null;
                default -> throw new IllegalArgumentException("Unsupported data type: " + targetType);
            };
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Cannot convert value '" + value + "' to type " + targetType);
        }
    }

    private void validateConstraints(TableMetadata metadata, Tuple tuple) {
        // Check NOT NULL constraints

        
        // Check UNIQUE constraints (handled by storage layer)
    }
} 