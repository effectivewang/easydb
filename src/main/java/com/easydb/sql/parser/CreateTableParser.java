package com.easydb.sql.parser;

import com.easydb.core.Column;
import com.easydb.core.DataType;
import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for CREATE TABLE statements.
 * Handles parsing of CREATE TABLE statements with column definitions and constraints.
 */
public class CreateTableParser extends Parser {
    private static final Pattern TABLE_PATTERN = Pattern.compile(
        "(?i)CREATE\\s+TABLE\\s+(\\w+)\\s*\\((.*?)\\)",
        Pattern.MULTILINE | Pattern.DOTALL
    );
    
    private static final Pattern COLUMN_PATTERN = Pattern.compile(
        "\\s*(\\w+)\\s+(?i)(INTEGER|STRING)\\s*(PRIMARY\\s+KEY)?\\s*(NOT\\s+NULL)?\\s*(?:,|$)",
        Pattern.MULTILINE | Pattern.DOTALL
    );

    public CreateTableParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParseTree parse() {
        // CREATE TABLE
        consume(TokenType.CREATE, "Expected 'CREATE' at start of statement");
        consume(TokenType.TABLE, "Expected 'TABLE' after 'CREATE'");
        ParseTree createNode = new ParseTree(ParseTreeType.CREATE_TABLE_STATEMENT);

        // Table name
        Token tableName = consume(TokenType.IDENTIFIER, "Expected table name");
        ParseTree tableRef = new ParseTree(ParseTreeType.TABLE_REF, tableName.value());
        createNode.addChild(tableRef);

        // Column definitions
        consume(TokenType.LEFT_PAREN, "Expected '(' after table name");
        ParseTree columnList = parseColumnDefinitions();
        createNode.addChild(columnList);

        // Semicolon (optional)
        match(TokenType.SEMICOLON);

        return createNode;
    }

    private ParseTree parseColumnDefinitions() {
        ParseTree columnList = new ParseTree(ParseTreeType.LIST);
        
        do {
            columnList.addChild(parseColumnDefinition());
        } while (match(TokenType.COMMA));

        consume(TokenType.RIGHT_PAREN, "Expected ')' after column definitions");
        return columnList;
    }

    private ParseTree parseColumnDefinition() {
        // Column name
        Token columnName = consume(TokenType.IDENTIFIER, "Expected column name");
        ParseTree columnDef = new ParseTree(ParseTreeType.COLUMN_REF, columnName.value());

        // Data type
        ParseTree dataType = parseDataType();
        columnDef.addChild(dataType);

        // Column constraints (optional)
        while (!check(TokenType.COMMA) && !check(TokenType.RIGHT_PAREN)) {
            columnDef.addChild(parseConstraint());
        }

        return columnDef;
    }

    private ParseTree parseDataType() {
        if (match(TokenType.INTEGER)) {
            return new ParseTree(ParseTreeType.INTEGER_TYPE);
        } else if (match(TokenType.STRING)) {
            return new ParseTree(ParseTreeType.STRING_TYPE);
        } else if (match(TokenType.BOOLEAN)) {
            return new ParseTree(ParseTreeType.BOOLEAN_TYPE);
        } else if (match(TokenType.DOUBLE)) {
            return new ParseTree(ParseTreeType.DOUBLE_TYPE);
        }
        throw error(peek(), "Expected data type");
    }

    private ParseTree parseConstraint() {
        if (match(TokenType.PRIMARY)) {
            consume(TokenType.KEY, "Expected 'KEY' after 'PRIMARY'");
            return new ParseTree(ParseTreeType.PRIMARY_KEY_CONSTRAINT);
        } else if (match(TokenType.FOREIGN)) {
            consume(TokenType.KEY, "Expected 'KEY' after 'FOREIGN'");
            consume(TokenType.REFERENCES, "Expected 'REFERENCES' after 'FOREIGN KEY'");
            Token tableName = consume(TokenType.IDENTIFIER, "Expected referenced table name");
            consume(TokenType.LEFT_PAREN, "Expected '('");
            Token columnName = consume(TokenType.IDENTIFIER, "Expected referenced column name");
            consume(TokenType.RIGHT_PAREN, "Expected ')'");

            ParseTree constraint = new ParseTree(ParseTreeType.FOREIGN_KEY_CONSTRAINT);
            constraint.addChild(new ParseTree(ParseTreeType.TABLE_REF, tableName.value()));
            constraint.addChild(new ParseTree(ParseTreeType.COLUMN_REF, columnName.value()));
            return constraint;
        } else if (match(TokenType.NOT)) {
            consume(TokenType.NULL, "Expected 'NULL' after 'NOT'");
            return new ParseTree(ParseTreeType.NOT_NULL_CONSTRAINT);
        } else if (match(TokenType.UNIQUE)) {
            return new ParseTree(ParseTreeType.UNIQUE_CONSTRAINT);
        }
        System.out.println(Arrays.toString(this.tokens.toArray()) + " : " + match(TokenType.UNIQUE));
        throw error(peek(), "Expected constraint");
    }

    private List<Column> parseColumns(String columnDefinitions) {
        List<Column> columns = new ArrayList<>();
        Matcher columnMatcher = COLUMN_PATTERN.matcher(columnDefinitions);

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
                null
            ));
        }

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("No valid columns defined");
        }

        return columns;
    }
} 