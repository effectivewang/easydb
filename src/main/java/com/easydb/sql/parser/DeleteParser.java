package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import java.util.List;

/**
 * Parser for SQL DELETE statements.
 * Handles parsing of DELETE FROM table [WHERE conditions] statements.
 */
public class DeleteParser extends Parser {
    
    public DeleteParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParseTree parse() {
        // DELETE
        consume(TokenType.DELETE, "Expected 'DELETE' at start of statement");
        ParseTree deleteNode = new ParseTree(ParseTreeType.DELETE_STATEMENT);

        // FROM
        consume(TokenType.FROM, "Expected 'FROM' after DELETE");

        // Table name
        Token tableName = consume(TokenType.IDENTIFIER, "Expected table name");
        ParseTree tableRef = new ParseTree(ParseTreeType.TABLE_REF, tableName.value());
        deleteNode.addChild(tableRef);

        // Optional WHERE clause
        if (match(TokenType.WHERE)) {
            ParseTree whereClause = parseWhereClause();
            deleteNode.addChild(whereClause);
        }

        // Optional semicolon
        match(TokenType.SEMICOLON);

        return deleteNode;
    }

    private ParseTree parseWhereClause() {
        ParseTree whereClause = new ParseTree(ParseTreeType.WHERE_CLAUSE);
        
        do {
            ParseTree condition = parseCondition();
            whereClause.addChild(condition);
        } while (match(TokenType.AND));

        return whereClause;
    }

    private ParseTree parseCondition() {
        ParseTree condition = new ParseTree(ParseTreeType.CONDITION);

        // Column name
        Token column = consume(TokenType.IDENTIFIER, "Expected column name");
        condition.addChild(new ParseTree(ParseTreeType.COLUMN_REF, column.value()));

        // Comparison operator
        Token operator = parseComparisonOperator();
        condition.addChild(new ParseTree(ParseTreeType.COMPARISON_OPERATOR, operator.value()));

        // Value
        condition.addChild(parseLiteral());

        return condition;
    }

    private Token parseComparisonOperator() {
        if (match(TokenType.EQUALS)) {
            return previous();
        } else if (match(TokenType.NOT_EQUALS)) {
            return previous();
        } else if (match(TokenType.LESS_THAN)) {
            return previous();
        } else if (match(TokenType.GREATER_THAN)) {
            return previous();
        } else if (match(TokenType.LESS_THAN_EQUALS)) {
            return previous();
        } else if (match(TokenType.GREATER_THAN_EQUALS)) {
            return previous();
        }
        throw error(peek(), "Expected comparison operator");
    }

    private ParseTree parseLiteral() {
        if (match(TokenType.STRING)) {
            return new ParseTree(ParseTreeType.LITERAL, previous().value());
        } else if (match(TokenType.INTEGER)) {
            return new ParseTree(ParseTreeType.LITERAL, previous().value());
        } else if (match(TokenType.DOUBLE)) {
            return new ParseTree(ParseTreeType.LITERAL, previous().value());
        } else if (match(TokenType.BOOLEAN)) {
            return new ParseTree(ParseTreeType.LITERAL, previous().value());
        } else if (match(TokenType.NULL)) {
            return new ParseTree(ParseTreeType.NULL_EXPR);
        }
        throw error(peek(), "Expected literal value");
    }
} 