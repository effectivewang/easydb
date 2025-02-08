package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;

import java.util.List;

/**
 * Parser for SELECT statements.
 * Handles parsing of SELECT queries including SELECT list, FROM clause, WHERE clause,
 * and other optional clauses.
 */
public class SelectParser extends Parser {
    public SelectParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParseTree parse() {
        // SELECT
        consume(TokenType.SELECT, "Expected 'SELECT' at start of query");
        ParseTree selectNode = new ParseTree(ParseTreeType.SELECT_STATEMENT);

        // SELECT list
        ParseTree selectList = parseSelectList();
        selectNode.addChild(selectList);

        // FROM clause
        if (match(TokenType.FROM)) {
            ParseTree fromClause = parseFromClause();
            selectNode.addChild(fromClause);
        }

        // WHERE clause (optional)
        if (match(TokenType.WHERE)) {
            ParseTree whereClause = parseWhereClause();
            selectNode.addChild(whereClause);
        }

        // GROUP BY clause (optional)
        if (match(TokenType.GROUP)) {
            consume(TokenType.BY, "Expected 'BY' after 'GROUP'");
            ParseTree groupByClause = parseGroupByClause();
            selectNode.addChild(groupByClause);

            // HAVING clause (optional, only valid with GROUP BY)
            if (match(TokenType.HAVING)) {
                ParseTree havingClause = parseHavingClause();
                selectNode.addChild(havingClause);
            }
        }

        // ORDER BY clause (optional)
        if (match(TokenType.ORDER)) {
            consume(TokenType.BY, "Expected 'BY' after 'ORDER'");
            ParseTree orderByClause = parseOrderByClause();
            selectNode.addChild(orderByClause);
        }

        // Semicolon (optional)
        match(TokenType.SEMICOLON);

        return selectNode;
    }

    private ParseTree parseSelectList() {
        ParseTree selectList = new ParseTree(ParseTreeType.SELECT_LIST);

        do {
            if (match(TokenType.MULTIPLY)) {
                selectList.addChild(new ParseTree(ParseTreeType.ASTERISK));
            } else {
                selectList.addChild(parseSelectItem());
            }
        } while (match(TokenType.COMMA));

        return selectList;
    }

    private ParseTree parseSelectItem() {
        ParseTree expr = parseExpression();

        // Check for alias (AS keyword is optional)
        if (match(TokenType.AS)) {
            Token as = consume(TokenType.AS, "Expected 'AS' after expression");
            Token alias = consume(TokenType.IDENTIFIER, "Expected alias name");
            ParseTree aliasNode = new ParseTree(ParseTreeType.ALIAS, alias.value());
            aliasNode.addChild(expr);
            return aliasNode;
        } else if (match(TokenType.IDENTIFIER)) {  
            Token identifier = consume(TokenType.IDENTIFIER, "Expected identifier");
            return new ParseTree(ParseTreeType.COLUMN_REF, identifier.value());
        }

        return expr;
    }

    private ParseTree parseFromClause() {
        ParseTree fromClause = new ParseTree(ParseTreeType.FROM_CLAUSE);
        
        do {
            ParseTree tableRef = parseTableReference();
            fromClause.addChild(tableRef);
        } while (match(TokenType.COMMA));

        return fromClause;
    }

    private ParseTree parseTableReference() {
        Token tableName = consume(TokenType.IDENTIFIER, "Expected table name");
        ParseTree tableRef = new ParseTree(ParseTreeType.TABLE_REF, tableName.value());

        // Check for alias
        if (match(TokenType.AS) || check(TokenType.IDENTIFIER)) {
            Token alias = consume(TokenType.IDENTIFIER, "Expected alias name");
            ParseTree aliasNode = new ParseTree(ParseTreeType.ALIAS, alias.value());
            aliasNode.addChild(tableRef);
            return aliasNode;
        }

        return tableRef;
    }

    private ParseTree parseWhereClause() {
        ParseTree whereClause = new ParseTree(ParseTreeType.WHERE_CLAUSE);
        whereClause.addChild(parseExpression());
        return whereClause;
    }

    private ParseTree parseGroupByClause() {
        ParseTree groupByClause = new ParseTree(ParseTreeType.GROUP_BY_CLAUSE);
        
        do {
            Token column = consume(TokenType.IDENTIFIER, "Expected column name");
            groupByClause.addChild(new ParseTree(ParseTreeType.COLUMN_REF, column.value()));
        } while (match(TokenType.COMMA));

        return groupByClause;
    }

    private ParseTree parseHavingClause() {
        ParseTree havingClause = new ParseTree(ParseTreeType.HAVING_CLAUSE);
        havingClause.addChild(parseExpression());
        return havingClause;
    }

    private ParseTree parseOrderByClause() {
        ParseTree orderByClause = new ParseTree(ParseTreeType.ORDER_BY_CLAUSE);
        
        do {
            Token column = consume(TokenType.IDENTIFIER, "Expected column name");
            ParseTree columnRef = new ParseTree(ParseTreeType.COLUMN_REF, column.value());
            orderByClause.addChild(columnRef);
        } while (match(TokenType.COMMA));

        return orderByClause;
    }

    private ParseTree parseExpression() {
        return parseLogicalOr();
    }

    private ParseTree parseLogicalOr() {
        ParseTree expr = parseLogicalAnd();

        while (match(TokenType.OR)) {
            Token operator = previous();
            ParseTree right = parseLogicalAnd();
            ParseTree newExpr = new ParseTree(ParseTreeType.BINARY_EXPR);
            newExpr.addChild(expr);
            newExpr.addChild(new ParseTree(ParseTreeType.OR_OPERATOR));
            newExpr.addChild(right);
            expr = newExpr;
        }

        return expr;
    }

    private ParseTree parseLogicalAnd() {
        ParseTree expr = parseEquality();

        while (match(TokenType.AND)) {
            Token operator = previous();
            ParseTree right = parseEquality();
            ParseTree newExpr = new ParseTree(ParseTreeType.BINARY_EXPR);
            newExpr.addChild(expr);
            newExpr.addChild(new ParseTree(ParseTreeType.AND_OPERATOR));
            newExpr.addChild(right);
            expr = newExpr;
        }

        return expr;
    }

    private ParseTree parseEquality() {
        ParseTree expr = parseComparison();

        while (match(TokenType.EQUALS, TokenType.NOT_EQUALS)) {
            Token operator = previous();
            ParseTree right = parseComparison();
            ParseTree newExpr = new ParseTree(ParseTreeType.BINARY_EXPR);
            newExpr.addChild(expr);
            newExpr.addChild(new ParseTree(operator.type() == TokenType.EQUALS ? 
                ParseTreeType.EQUALS_OPERATOR : ParseTreeType.NOT_EQUALS_OPERATOR));
            newExpr.addChild(right);
            expr = newExpr;
        }

        return expr;
    }

    private ParseTree parseComparison() {
        ParseTree expr = parseTerm();

        while (match(TokenType.LESS_THAN, TokenType.LESS_THAN_EQUALS,
                    TokenType.GREATER_THAN, TokenType.GREATER_THAN_EQUALS)) {
            Token operator = previous();
            ParseTree right = parseTerm();
            ParseTree newExpr = new ParseTree(ParseTreeType.BINARY_EXPR);
            newExpr.addChild(expr);
            newExpr.addChild(new ParseTree(getComparisonOperatorType(operator.type())));
            newExpr.addChild(right);
            expr = newExpr;
        }

        return expr;
    }

    private ParseTree parseTerm() {
        ParseTree expr = parseFactor();

        while (match(TokenType.PLUS, TokenType.MINUS)) {
            Token operator = previous();
            ParseTree right = parseFactor();
            ParseTree newExpr = new ParseTree(ParseTreeType.BINARY_EXPR);
            newExpr.addChild(expr);
            newExpr.addChild(new ParseTree(operator.type() == TokenType.PLUS ? 
                ParseTreeType.PLUS_OPERATOR : ParseTreeType.MINUS_OPERATOR));
            newExpr.addChild(right);
            expr = newExpr;
        }

        return expr;
    }

    private ParseTree parseFactor() {
        ParseTree expr = parsePrimary();

        while (match(TokenType.MULTIPLY, TokenType.DIVIDE)) {
            Token operator = previous();
            ParseTree right = parsePrimary();
            ParseTree newExpr = new ParseTree(ParseTreeType.BINARY_EXPR);
            newExpr.addChild(expr);
            newExpr.addChild(new ParseTree(operator.type() == TokenType.MULTIPLY ? 
                ParseTreeType.MULTIPLY_OPERATOR : ParseTreeType.DIVIDE_OPERATOR));
            newExpr.addChild(right);
            expr = newExpr;
        }

        return expr;
    }

    private ParseTree parsePrimary() {
        if (match(TokenType.NUMBER_LITERAL)) {
            return new ParseTree(ParseTreeType.LITERAL, previous().value());
        }

        if (match(TokenType.STRING_LITERAL)) {
            return new ParseTree(ParseTreeType.LITERAL, previous().value());
        }

        if (match(TokenType.IDENTIFIER)) {
            return new ParseTree(ParseTreeType.COLUMN_REF, previous().value());
        }

        if (match(TokenType.LEFT_PAREN)) {
            ParseTree expr = parseExpression();
            consume(TokenType.RIGHT_PAREN, "Expected ')' after expression");
            return expr;
        }

        throw error(peek(), "Expected expression");
    }

    private ParseTreeType getComparisonOperatorType(TokenType tokenType) {
        switch (tokenType) {
            case LESS_THAN:
                return ParseTreeType.LESS_THAN_OPERATOR;
            case LESS_THAN_EQUALS:
                return ParseTreeType.LESS_THAN_EQUALS_OPERATOR;
            case GREATER_THAN:
                return ParseTreeType.GREATER_THAN_OPERATOR;
            case GREATER_THAN_EQUALS:
                return ParseTreeType.GREATER_THAN_EQUALS_OPERATOR;
            default:
                throw new IllegalStateException("Unexpected operator type: " + tokenType);
        }
    }
} 