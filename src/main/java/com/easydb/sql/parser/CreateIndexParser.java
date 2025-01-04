package com.easydb.sql.parser;

import com.easydb.sql.command.CreateIndexCommand;
import com.easydb.sql.command.SqlCommand;
import com.easydb.storage.metadata.IndexType;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CreateIndexParser implements SqlParser {
    private static final Pattern INDEX_PATTERN = Pattern.compile(
        "CREATE\\s+(UNIQUE\\s+)?INDEX\\s+(\\w+)\\s+ON\\s+(\\w+)\\s*\\(([^)]+)\\)(?:\\s+USING\\s+(HASH|BTREE|GIN))?",
        Pattern.CASE_INSENSITIVE
    );

    @Override
    public SqlCommand parse(String sql) {
        Matcher matcher = INDEX_PATTERN.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid CREATE INDEX syntax");
        }

        boolean isUnique = matcher.group(1) != null;
        String indexName = matcher.group(2);
        String tableName = matcher.group(3);
        List<String> columnNames = parseColumnNames(matcher.group(4));
        IndexType indexType = parseIndexType(matcher.group(5));

        return new CreateIndexCommand(
            indexName,
            tableName,
            columnNames,
            isUnique,
            indexType
        );
    }

    private List<String> parseColumnNames(String columnsStr) {
        return Arrays.stream(columnsStr.split(","))
            .map(String::trim)
            .collect(Collectors.toList());
    }

    private IndexType parseIndexType(String type) {
        if (type == null || type.equalsIgnoreCase("HASH")) {
            return IndexType.HASH;
        } else if (type.equalsIgnoreCase("BTREE")) {
            return IndexType.BTREE;
        } else if (type.equalsIgnoreCase("GIN")) {
            return IndexType.GIN;
        }
        throw new IllegalArgumentException("Unsupported index type: " + type);
    }
} 