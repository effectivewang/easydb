package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for SQL statement parsers.
 * Provides common functionality for parsing SQL statements.
 */
public abstract class Parser {
    protected final List<Token> tokens;
    protected int current;

    protected Parser(List<Token> tokens) {
        this.tokens = tokens;
        this.current = 0;
    }

    /**
     * Parse the tokens into a parse tree.
     * Each subclass must implement this method to handle specific statement types.
     */
    public abstract ParseTree parse();

    /**
     * Check if the current token matches any of the given types.
     */
    protected boolean match(TokenType... types) {
        for (TokenType type : types) {
            if (check(type)) {
                advance();
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the current token is of the given type.
     */
    protected boolean check(TokenType type) {
        if (isAtEnd()) return false;
        return peek().type() == type;
    }

    /**
     * Consume the current token and return it.
     */
    protected Token advance() {
        if (!isAtEnd()) current++;
        return previous();
    }

    /**
     * Check if we've reached the end of the token list.
     */
    protected boolean isAtEnd() {
        return peek().type() == TokenType.EOF;
    }

    /**
     * Get the current token without consuming it.
     */
    protected Token peek() {
        return tokens.get(current);
    }

    /**
     * Get the previous token.
     */
    protected Token previous() {
        return tokens.get(current - 1);
    }

    /**
     * Consume a token of the expected type or throw an error.
     */
    protected Token consume(TokenType type, String message) {
        if (check(type)) return advance();
        throw error(peek(), message);
    }

    /**
     * Create a parse error with the given message.
     */
    protected ParseException error(Token token, String message) {
        return new ParseException(token, message);
    }

    /**
     * Synchronize the parser state after an error.
     * Skip tokens until we find a likely statement boundary.
     */
    protected void synchronize() {
        advance();

        while (!isAtEnd()) {
            if (previous().type() == TokenType.SEMICOLON) return;

            switch (peek().type()) {
                case SELECT:
                case INSERT:
                case UPDATE:
                case DELETE:
                case CREATE:
                case DROP:
                    return;
                default:
                    advance();
            }
        }
    }

    /**
     * Parse a comma-separated list of items using the provided parser function.
     */
    protected List<ParseTree> parseList(TokenType delimiter, ParseFunction parser) {
        List<ParseTree> items = new ArrayList<>();
        
        do {
            items.add(parser.parse());
        } while (match(delimiter));

        return items;
    }

    @FunctionalInterface
    protected interface ParseFunction {
        ParseTree parse();
    }
} 