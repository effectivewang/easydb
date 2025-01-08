package com.easydb.sql;

import com.easydb.storage.InMemoryStorage;
import com.easydb.core.Tuple;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.core.Column;
import com.easydb.core.Transaction;
import com.easydb.core.IsolationLevel;
import com.easydb.sql.parser.SqlParserFactory;
import com.easydb.sql.command.SqlCommand;
import com.easydb.sql.result.ResultSet;
import com.easydb.storage.transaction.TransactionManager;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DefaultSqlEngine implements SqlEngine {
    private final InMemoryStorage storage;
    private final TransactionManager transactionManager;
    private final SqlParserFactory sqlParserFactory;

    public DefaultSqlEngine(InMemoryStorage storage, TransactionManager transactionManager) {
        this.storage = storage;
        this.sqlParserFactory = new SqlParserFactory(storage, transactionManager);
        this.transactionManager = transactionManager;
    }

    @Override
    public CompletableFuture<Object> execute(SqlCommand command) {
        return command.execute(storage);
    }
    
    public CompletableFuture<Object> execute(SqlCommand command, Transaction txn) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return command.execute(storage, txn).join();
            } catch (Exception e) {
                throw e;
            }
        });
    }

    @Override
    public CompletableFuture<Integer> executeUpdate(String sql) {
        return CompletableFuture.supplyAsync(() -> {
            SqlCommand cmd = sqlParserFactory.parse(sql);
            cmd.execute(storage).join();
            return 1;
        });
    }

    public CompletableFuture<Integer> executeUpdate(String sql, Transaction txn) {
        return CompletableFuture.supplyAsync(() -> {
            SqlCommand cmd = sqlParserFactory.parse(sql);
            cmd.execute(storage, txn).join();
            return 1;
        });
    }

    @Override
    public CompletableFuture<ResultSet> executeQuery(String sql) {
        return CompletableFuture.supplyAsync(() -> {
            // Get table metadata
            String tableName = extractTableName(sql);
            TableMetadata metadata = storage.getTableMetadata(tableName).join();
            List<Column> columns = metadata.columns();

            // Get conditions
            Map<String, Object> conditions = extractConditions(sql);

            // Find tuples
            List<Tuple> tuples = storage.findTuples(tableName, conditions).join();

            // Convert tuples to ResultSet
            ResultSet.Builder builder = new ResultSet.Builder();
            
            // Add columns
            for (Column column : columns) {
                builder.addColumn(column);
            }

            // Add rows
            for (Tuple tuple : tuples) {
                List<Object> values = tuple.getValues(columns.stream()
                    .map(col -> col.type().getJavaType())
                    .collect(Collectors.toList()));

                Map<String, Object> rowMap = new HashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    rowMap.put(columns.get(i).name(), values.get(i));
                }
                builder.addRow(rowMap);
            }

            return builder.build();
        });
    }

    private String extractTableName(String sql) {
        // Simple implementation - extract table name from "FROM" clause
        String[] parts = sql.split("\\s+");
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase("FROM") && i + 1 < parts.length) {
                return parts[i + 1];
            }
        }
        throw new IllegalArgumentException("Invalid SQL: missing FROM clause");
    }

    private Map<String, Object> extractConditions(String sql) {
        // Simple implementation - extract conditions from "WHERE" clause
        Map<String, Object> conditions = new HashMap<>();
        String[] parts = sql.split("\\s+");
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].equalsIgnoreCase("WHERE") && i + 3 < parts.length) {
                String column = parts[i + 1];
                String operator = parts[i + 2];
                String value = parts[i + 3];
                if (operator.equals("=")) {
                    conditions.put(column, parseValue(value));
                }
            }
        }
        return conditions;
    }

    private Object parseValue(String value) {
        // Simple implementation - parse string value to appropriate type
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e2) {
                if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                    return Boolean.parseBoolean(value);
                }
                return value;
            }
        }
    }
} 