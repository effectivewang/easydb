package com.easydb.sql.parser;

import com.easydb.sql.command.SelectCommand;
import com.easydb.sql.command.SqlCommand;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for SQL SELECT statements.
 */
public class SelectParser implements SqlParser {
    private static final Pattern SELECT_PATTERN = Pattern.compile(
        "SELECT\\s+([\\w\\s,*]+)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+))?",
        Pattern.CASE_INSENSITIVE
    );

    @Override
    public SqlCommand parse(String sql) {
        Matcher matcher = SELECT_PATTERN.matcher(sql.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid SELECT statement: " + sql);
        }

        String columnsStr = matcher.group(1).trim();
        String tableName = matcher.group(2).trim();
        String whereClause = matcher.group(3);

        List<String> columns = parseColumns(columnsStr);
        Map<String, Object> conditions = parseConditions(whereClause);

        return new SelectCommand(tableName, columns, conditions);
    }

    private List<String> parseColumns(String columnsStr) {
        if (columnsStr.equals("*")) {
            return Collections.emptyList();
        }
        return Arrays.stream(columnsStr.split("\\s*,\\s*"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();
    }

    private Map<String, Object> parseConditions(String whereClause) {
        if (whereClause == null || whereClause.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Object> conditions = new HashMap<>();
        String[] parts = whereClause.split("\\s+AND\\s+");
        for (String part : parts) {
            String[] condition = part.split("\\s*=\\s*");
            if (condition.length != 2) {
                throw new IllegalArgumentException("Invalid condition: " + part);
            }
            String key = condition[0].trim();
            String value = condition[1].trim();
            
            // Remove quotes if present
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            
            // Try to parse as number if possible
            try {
                if (value.contains(".")) {
                    conditions.put(key, Double.parseDouble(value));
                } else {
                    conditions.put(key, Long.parseLong(value));
                }
            } catch (NumberFormatException e) {
                conditions.put(key, value);
            }
        }
        return conditions;
    }
} 