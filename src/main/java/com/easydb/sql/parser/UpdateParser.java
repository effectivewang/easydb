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
        whereClause.addChild(parseCondition());
        return whereClause;
    }

    private ParseTree parseCondition() {
        ParseTree left = parseComparison();

        while (match(TokenType.AND) || match(TokenType.OR)) {
            Token operator = previous();
            ParseTree right = parseComparison();
            
            ParseTree condition = new ParseTree(ParseTreeType.CONDITION);
            condition.addChild(left);
            condition.addChild(new ParseTree(
                operator.type() == TokenType.AND ? 
                ParseTreeType.AND_OPERATOR : 
                ParseTreeType.OR_OPERATOR
            ));
            condition.addChild(right);
            left = condition;
        }

        return left;
    }

    private ParseTree parseComparison() {
        ParseTree left = parseExpression();

        if (match(TokenType.EQUALS) || match(TokenType.NOT_EQUALS) || 
            match(TokenType.GREATER_THAN) || match(TokenType.LESS_THAN) ||
            match(TokenType.GREATER_THAN_EQUALS) || match(TokenType.LESS_THAN_EQUALS)) {
            
            Token operator = previous();
            ParseTree right = parseExpression();
            
            ParseTree comparison = new ParseTree(ParseTreeType.COMPARISON_OPERATOR);
            comparison.addChild(left);
            comparison.addChild(new ParseTree(getComparisonType(operator)));
            comparison.addChild(right);
            return comparison;
        }

        return left;
    }

    private ParseTreeType getComparisonType(Token operator) {
        return switch (operator.type()) {
            case EQUALS -> ParseTreeType.EQUALS_OPERATOR;
            case NOT_EQUALS -> ParseTreeType.NOT_EQUALS_OPERATOR;
            case GREATER_THAN -> ParseTreeType.GREATER_THAN_OPERATOR;
            case LESS_THAN -> ParseTreeType.LESS_THAN_OPERATOR;
            case GREATER_THAN_EQUALS -> ParseTreeType.GREATER_THAN_EQUALS_OPERATOR;
            case LESS_THAN_EQUALS -> ParseTreeType.LESS_THAN_EQUALS_OPERATOR;
            default -> throw new IllegalStateException("Unknown comparison operator: " + operator.type());
        };
    }
} 