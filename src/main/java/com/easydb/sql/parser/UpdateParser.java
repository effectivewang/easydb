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

            // Value or Expression
            assignment.addChild(parseExpression());
            setList.addChild(assignment);
        } while (match(TokenType.COMMA));

        return setList;
    }

    private ParseTree parseExpression() {
        ParseTree left = parseTerm();

        while (match(TokenType.PLUS) || match(TokenType.MINUS)) {
            Token operator = previous();
            ParseTree right = parseTerm();
            
            ParseTree expr = new ParseTree(ParseTreeType.ARITHMETIC_EXPR);
            expr.addChild(left);
            expr.addChild(new ParseTree(operator.type() == TokenType.PLUS ? 
                ParseTreeType.PLUS_OPERATOR : ParseTreeType.MINUS_OPERATOR));
            expr.addChild(right);
            left = expr;
        }

        return left;
    }

    private ParseTree parseTerm() {
        ParseTree left = parseFactor();

        while (match(TokenType.MULTIPLY) || match(TokenType.DIVIDE)) {
            Token operator = previous();
            ParseTree right = parseFactor();
            
            ParseTree expr = new ParseTree(ParseTreeType.ARITHMETIC_EXPR);
            expr.addChild(left);
            expr.addChild(new ParseTree(operator.type() == TokenType.MULTIPLY ? 
                ParseTreeType.MULTIPLY_OPERATOR : ParseTreeType.DIVIDE_OPERATOR));
            expr.addChild(right);
            left = expr;
        }

        return left;
    }

    private ParseTree parseFactor() {
        if (match(TokenType.LEFT_PAREN)) {
            ParseTree expr = parseExpression();
            consume(TokenType.RIGHT_PAREN, "Expected ')'");
            return expr;
        }

        if (match(TokenType.IDENTIFIER)) {
            return new ParseTree(ParseTreeType.COLUMN_REF, previous().value());
        }

        return parseLiteral();
    }

    private ParseTree parseLiteral() {
        if (match(TokenType.STRING)) {
            return new ParseTree(ParseTreeType.STRING_TYPE, previous().value());
        } else if (match(TokenType.INTEGER)) {
            return new ParseTree(ParseTreeType.INTEGER_TYPE, previous().value());
        } else if (match(TokenType.DOUBLE)) {
            return new ParseTree(ParseTreeType.DOUBLE_TYPE, previous().value());
        } else if (match(TokenType.BOOLEAN)) {
            return new ParseTree(ParseTreeType.BOOLEAN_TYPE, previous().value());
        } else if (match(TokenType.NULL)) {
            return new ParseTree(ParseTreeType.NULL_TYPE);
        }
        throw error(peek(), "Expected literal value");
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
} 