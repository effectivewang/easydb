package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;

import java.util.List;

/**
 * Parser for INSERT statements.
 * Handles parsing of INSERT INTO table VALUES (...) statements.
 */
public class InsertParser extends Parser {
    public InsertParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParseTree parse() {
        // INSERT
        consume(TokenType.INSERT, "Expected 'INSERT' at start of statement");
        ParseTree insertNode = new ParseTree(ParseTreeType.INSERT_STATEMENT);

        // INTO
        consume(TokenType.INTO, "Expected 'INTO' after 'INSERT'");

        // Table name
        Token tableName = consume(TokenType.IDENTIFIER, "Expected table name");
        ParseTree tableRef = new ParseTree(ParseTreeType.TABLE_REF, tableName.value());
        insertNode.addChild(tableRef);

        // Optional column list
        ParseTree columnList = null;
        if (match(TokenType.LEFT_PAREN)) {
            columnList = parseColumnList();
            insertNode.addChild(columnList);
        }

        // VALUES
        consume(TokenType.VALUES, "Expected 'VALUES'");
        ParseTree valuesList = parseValuesList();
        insertNode.addChild(valuesList);

        // Semicolon (optional)
        match(TokenType.SEMICOLON);

        return insertNode;
    }

    private ParseTree parseColumnList() {
        ParseTree columnList = new ParseTree(ParseTreeType.LIST);
        
        do {
            Token column = consume(TokenType.IDENTIFIER, "Expected column name");
            columnList.addChild(new ParseTree(ParseTreeType.COLUMN_REF, column.value()));
        } while (match(TokenType.COMMA));

        consume(TokenType.RIGHT_PAREN, "Expected ')' after column list");
        return columnList;
    }

    private ParseTree parseValuesList() {
        ParseTree valuesList = new ParseTree(ParseTreeType.VALUES_CLAUSE);

        do {
            consume(TokenType.LEFT_PAREN, "Expected '('");
            ParseTree valueList = new ParseTree(ParseTreeType.LIST);
            
            do {
                valueList.addChild(parseLiteral());
            } while (match(TokenType.COMMA));

            consume(TokenType.RIGHT_PAREN, "Expected ')'");
            valuesList.addChild(valueList);
        } while (match(TokenType.COMMA));

        return valuesList;
    }

    private ParseTree parseLiteral() {
        if (match(TokenType.STRING_LITERAL)) {
            return new ParseTree(ParseTreeType.STRING_TYPE, previous().value());
        } else if (match(TokenType.INTEGER)) {
            return new ParseTree(ParseTreeType.INTEGER_TYPE, previous().value());
        } else if (match(TokenType.DOUBLE)) {
            return new ParseTree(ParseTreeType.DOUBLE_TYPE, previous().value());
        } else if (match(TokenType.NULL)) {
            return new ParseTree(ParseTreeType.NULL_EXPR);
        }
        throw error(peek(), "Expected literal value");
    }
} 