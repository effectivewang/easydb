package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.easydb.storage.Storage;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.core.Column;

/**
 * Parser for SQL UPDATE statements.
 * Handles parsing of UPDATE table SET col = val [WHERE conditions] statements.
 */
public class UpdateParser extends Parser {
    
    public UpdateParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParseTree parse() {
        // UPDATE
        consume(TokenType.UPDATE, "Expected 'UPDATE' at start of statement");
        ParseTree updateNode = new ParseTree(ParseTreeType.UPDATE_STATEMENT);

        // Table name
        Token tableName = consume(TokenType.IDENTIFIER, "Expected table name");
        ParseTree tableRef = new ParseTree(ParseTreeType.TABLE_REF, tableName.value());
        updateNode.addChild(tableRef);

        // SET
        consume(TokenType.SET, "Expected 'SET' after table name");
        ParseTree setList = parseSetClause();
        updateNode.addChild(setList);

        // Optional WHERE clause
        if (match(TokenType.WHERE)) {
            ParseTree whereClause = parseWhereClause();
            updateNode.addChild(whereClause);
        }

        // Optional semicolon
        match(TokenType.SEMICOLON);

        return updateNode;
    }

    private ParseTree parseSetClause() {
        ParseTree setList = new ParseTree(ParseTreeType.SET_CLAUSE);
        
        do {
            // Column name
            Token column = consume(TokenType.IDENTIFIER, "Expected column name");
            ParseTree assignment = new ParseTree(ParseTreeType.ASSIGNMENT);
            assignment.addChild(new ParseTree(ParseTreeType.COLUMN_REF, column.value()));

            // Equals sign
            consume(TokenType.EQUALS, "Expected '=' after column name");

            // Value
            assignment.addChild(parseLiteral());
            setList.addChild(assignment);
        } while (match(TokenType.COMMA));

        return setList;
    }

    private ParseTree parseWhereClause() {
        ParseTree whereClause = new ParseTree(ParseTreeType.WHERE_CLAUSE);
        
        do {
            // Column name
            Token column = consume(TokenType.IDENTIFIER, "Expected column name");
            ParseTree condition = new ParseTree(ParseTreeType.CONDITION);
            condition.addChild(new ParseTree(ParseTreeType.COLUMN_REF, column.value()));

            // Operator
            consume(TokenType.EQUALS, "Expected '=' in WHERE clause");
            condition.addChild(new ParseTree(ParseTreeType.EQUALS_OPERATOR, "="));

            // Value
            condition.addChild(parseLiteral());
            whereClause.addChild(condition);
        } while (match(TokenType.AND));

        return whereClause;
    }

    private ParseTree parseLiteral() {
        if (match(TokenType.STRING_LITERAL, TokenType.NUMBER_LITERAL)) {
            return new ParseTree(ParseTreeType.LITERAL, previous().value());
        } else if (match(TokenType.NULL)) {
            return new ParseTree(ParseTreeType.NULL_EXPR);
        }
        throw error(peek(), "Expected literal value");
    }
} 