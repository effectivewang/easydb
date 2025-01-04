package com.easydb.sql.parser;

import com.easydb.sql.command.InsertCommand;
import com.easydb.sql.command.SqlCommand;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.easydb.storage.Storage;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.core.Column;

/**
 * Parser for SQL INSERT statements.
 */
public class InsertParser implements SqlParser {
    private static final Pattern INSERT_PATTERN = Pattern.compile(
        "INSERT\\s+INTO\\s+(\\w+)\\s*\\((.*?)\\)\\s*VALUES\\s*\\((.*?)\\)",
        Pattern.CASE_INSENSITIVE
    );

    private final Storage storage;

    public InsertParser(Storage storage) {
        this.storage = storage;
    }

    @Override
    public SqlCommand parse(String sql) {
        Matcher matcher = INSERT_PATTERN.matcher(sql.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid INSERT statement: " + sql);
        }

        String tableName = matcher.group(1).trim();
        TableMetadata tableMetadata = storage.getTableMetadata(tableName).join();
        List<String> columns = parseColumns(matcher.group(2));
        List<List<Object>> values = parseValues(tableMetadata, columns, matcher.group(3));

        return new InsertCommand(tableName, columns, values);
    }

    private List<String> parseColumns(String columnsStr) {
        return Arrays.stream(columnsStr.split("\\s*,\\s*"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();
    }

    private List<List<Object>> parseValues(TableMetadata tableMetadata, List<String> columns, String valuesStr) {
        List<List<Object>> result = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        
        String[] parts = valuesStr.split("\\s*,\\s*");

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            String columnName = columns.get(i);
            
            // Remove quotes if present
            if (part.startsWith("'") && part.endsWith("'")) {
                part = part.substring(1, part.length() - 1);
            }
            
            Column column = tableMetadata.columns().stream()
                .filter(c -> c.name().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Column " + columnName + " not found in table " + tableMetadata.tableName()));
            values.add(column.parseValue(part));
        }
        
        result.add(values);
        return result;
    }
} 