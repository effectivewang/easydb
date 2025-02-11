package com.easydb.sql.parser;

import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import com.easydb.index.IndexType;
import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;


import java.util.List;

/**
 * Parser for CREATE INDEX statements.
 * Handles parsing of CREATE [UNIQUE] INDEX name ON table (columns) [USING method] statements.
 */
public class CreateIndexParser extends Parser {
    
    public CreateIndexParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParseTree parse() {
        // CREATE
        consume(TokenType.CREATE, "Expected 'CREATE' at start of statement");
        ParseTree createNode = new ParseTree(ParseTreeType.CREATE_INDEX_STATEMENT);

        // Optional UNIQUE
        boolean isUnique = match(TokenType.UNIQUE);
        if (isUnique) {
            createNode.addChild(new ParseTree(ParseTreeType.UNIQUE_CONSTRAINT));
        }

        // INDEX
        consume(TokenType.INDEX, "Expected 'INDEX'");

        // Index name
        Token indexName = consume(TokenType.IDENTIFIER, "Expected index name");
        ParseTree indexNameNode = new ParseTree(ParseTreeType.INDEX_REF, indexName.value());
        createNode.addChild(indexNameNode);

        // ON
        consume(TokenType.ON, "Expected 'ON' after index name");

        // Table name
        Token tableName = consume(TokenType.IDENTIFIER, "Expected table name");
        ParseTree tableRef = new ParseTree(ParseTreeType.TABLE_REF, tableName.value());
        createNode.addChild(tableRef);

        // Column list
        consume(TokenType.LEFT_PAREN, "Expected '(' after table name");
        ParseTree columnList = parseColumnList();
        createNode.addChild(columnList);

        // Optional USING clause
        ParseTree indexTypeNode = parseIndexType();
        if (indexTypeNode != null) {
            createNode.addChild(indexTypeNode);
        }

        // Optional semicolon
        match(TokenType.SEMICOLON);

        return createNode;
    }

    private ParseTree parseColumnList() {
        ParseTree columnList = new ParseTree(ParseTreeType.COLUMN_REF);
        
        do {
            Token column = consume(TokenType.IDENTIFIER, "Expected column name");
            columnList.addChild(new ParseTree(ParseTreeType.COLUMN_REF, column.value()));
        } while (match(TokenType.COMMA));

        consume(TokenType.RIGHT_PAREN, "Expected ')' after column list");
        return columnList;
    }

    private ParseTree parseIndexType() {
        if (match(TokenType.USING)) {
            Token type = consume(TokenType.IDENTIFIER, "Expected index type after USING");
            String indexType = type.value().toUpperCase();
            
            // Validate index type
            try {
                Enum.valueOf(IndexType.class, indexType);
            } catch (IllegalArgumentException e) {
                throw error(type, "Invalid index type: " + indexType);
            }
            
            return new ParseTree(ParseTreeType.USING_CLAUSE, indexType);
        }
        return null;
    }
} 