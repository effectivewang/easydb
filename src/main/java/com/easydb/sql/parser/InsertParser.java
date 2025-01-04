package com.easydb.sql.parser;

import com.easydb.sql.command.InsertCommand;
import com.easydb.sql.command.SqlCommand;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for SQL INSERT statements.
 */
public class InsertParser implements SqlParser {
    private static final Pattern INSERT_PATTERN = Pattern.compile(
        "INSERT\\s+INTO\\s+(\\w+)\\s*\\((.*?)\\)\\s*VALUES\\s*\\((.*?)\\)",
        Pattern.CASE_INSENSITIVE
    );

    @Override
    public SqlCommand parse(String sql) {
        Matcher matcher = INSERT_PATTERN.matcher(sql.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid INSERT statement: " + sql);
        }

        String tableName = matcher.group(1).trim();
        List<String> columns = parseColumns(matcher.group(2));
        List<List<Object>> values = parseValues(matcher.group(3));

        return new InsertCommand(tableName, columns, values);
    }

    private List<String> parseColumns(String columnsStr) {
        return Arrays.stream(columnsStr.split("\\s*,\\s*"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();
    }

    private List<List<Object>> parseValues(String valuesStr) {
        List<List<Object>> result = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        
        String[] parts = valuesStr.split("\\s*,\\s*");
        for (String part : parts) {
            String value = part.trim();
            
            // Remove quotes if present
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            
            // Try to parse as number if possible
            try {
                if (value.equalsIgnoreCase("null")) {
                    values.add(null);
                } else {
                    values.add(Integer.parseInt(value));
                }
            } catch (NumberFormatException e) {
                values.add(value);
            }
        }
        
        result.add(values);
        return result;
    }
} 