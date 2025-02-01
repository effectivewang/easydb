package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;

/**
 * Exception thrown when an error occurs during SQL parsing.
 */
public class ParseException extends RuntimeException {
    private final Token token;

    public ParseException(Token token, String message) {
        super(String.format("Error at position %d: %s", token.position(), message));
        this.token = token;
    }

    public Token getToken() {
        return token;
    }

    public int getPosition() {
        return token.position();
    }

    @Override
    public String toString() {
        return String.format("ParseException: %s [at position %d, token: %s]",
            getMessage(), token.position(), token.value());
    }
} 