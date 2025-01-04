package com.easydb.sql.parser;

import com.easydb.core.Column;
import com.easydb.core.DataType;
import com.easydb.sql.command.CreateTableCommand;
import com.easydb.sql.command.SqlCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateTableParser implements SqlParser {
    private static final Pattern TABLE_PATTERN = Pattern.compile(
        "(?i)CREATE\\s+TABLE\\s+(\\w+)\\s*\\((.*?)\\)",
        Pattern.MULTILINE | Pattern.DOTALL
    );
    
    private static final Pattern COLUMN_PATTERN = Pattern.compile(
        "\\s*(\\w+)\\s+(?i)(INTEGER|STRING)\\s*(PRIMARY\\s+KEY)?\\s*(NOT\\s+NULL)?\\s*(?:,|$)",
        Pattern.MULTILINE | Pattern.DOTALL
    );

    @Override
    public SqlCommand parse(String sql) {
        sql = sql.replaceAll("\\s+", " ")
                 .replaceAll("\\s*,\\s*", ", ")
                 .trim();

        Matcher tableMatcher = TABLE_PATTERN.matcher(sql);
        if (!tableMatcher.find()) {
            throw new IllegalArgumentException("Invalid CREATE TABLE syntax");
        }

        String tableName = tableMatcher.group(1);
        String columnDefinitions = tableMatcher.group(2).trim();
        List<Column> columns = parseColumns(columnDefinitions);

        return new CreateTableCommand(tableName, columns);
    }

    private List<Column> parseColumns(String columnDefinitions) {
        List<Column> columns = new ArrayList<>();
        Matcher columnMatcher = COLUMN_PATTERN.matcher(columnDefinitions);
        int position = 0;

        while (columnMatcher.find()) {
            String name = columnMatcher.group(1);
            String type = columnMatcher.group(2).toUpperCase();
            boolean isPrimaryKey = columnMatcher.group(3) != null;
            boolean isNotNull = columnMatcher.group(4) != null || isPrimaryKey;

            columns.add(new Column(
                name,
                DataType.valueOf(type),
                isNotNull,
                isPrimaryKey,
                false,
                null,
                position++
            ));
        }

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("No valid columns defined");
        }

        return columns;
    }
} 