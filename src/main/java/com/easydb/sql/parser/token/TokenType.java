package com.easydb.sql.parser.token;

/**
 * Types of tokens in SQL statements.
 */
public enum TokenType {
    // Keywords
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    CREATE,
    DROP,
    TABLE,
    INDEX,
    INTO,
    VALUES,
    FROM,
    WHERE,
    AND,
    OR,
    NOT,
    NULL,
    PRIMARY,
    KEY,
    FOREIGN,
    REFERENCES,
    GROUP,
    BY,
    HAVING,
    ORDER,
    ASC,
    DESC,
    SET,
    AS,
    BETWEEN,
    LIKE,
    ILIKE,
    LIMIT,
    OFFSET,
    DISTINCT,
    UNION,
    INTERSECT,
    EXCEPT,
    UNIQUE,
    ON,
    USING,
    
    
    // Data types
    INTEGER,
    STRING,
    BOOLEAN,
    DOUBLE,
    
    // Operators
    EQUALS("="),
    LESS_THAN("<"),
    GREATER_THAN(">"),
    LESS_THAN_EQUALS("<="),
    GREATER_THAN_EQUALS(">="),
    NOT_EQUALS("!="),
    PLUS("+"),
    MINUS("-"),
    MULTIPLY("*"),
    DIVIDE("/"),
    
    // Punctuation
    LEFT_PAREN("("),
    RIGHT_PAREN(")"),
    COMMA(","),
    SEMICOLON(";"),
    DOT("."),
    
    // Other
    IDENTIFIER,
    EOF;

    private final String symbol;

    TokenType() {
        this.symbol = null;
    }

    TokenType(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }
} 