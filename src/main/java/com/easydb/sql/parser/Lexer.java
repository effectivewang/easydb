package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import java.util.*;

/**
 * SQL Lexer that converts raw SQL text into a stream of tokens.
 */
public class Lexer {
    private final String input;
    private int position;
    private int start;
    private final List<Token> tokens;
    private static final Map<String, TokenType> KEYWORDS;

    static {
        KEYWORDS = new HashMap<>();
        KEYWORDS.put("SELECT", TokenType.SELECT);
        KEYWORDS.put("INSERT", TokenType.INSERT);
        KEYWORDS.put("UPDATE", TokenType.UPDATE);
        KEYWORDS.put("DELETE", TokenType.DELETE);
        KEYWORDS.put("CREATE", TokenType.CREATE);
        KEYWORDS.put("DROP", TokenType.DROP);
        KEYWORDS.put("TABLE", TokenType.TABLE);
        KEYWORDS.put("INDEX", TokenType.INDEX);
        KEYWORDS.put("INTO", TokenType.INTO);
        KEYWORDS.put("VALUES", TokenType.VALUES);
        KEYWORDS.put("FROM", TokenType.FROM);
        KEYWORDS.put("WHERE", TokenType.WHERE);
        KEYWORDS.put("ORDER", TokenType.ORDER);
        KEYWORDS.put("GROUP", TokenType.GROUP);
        KEYWORDS.put("ON", TokenType.ON);
        KEYWORDS.put("USING", TokenType.USING);
        KEYWORDS.put(">", TokenType.GREATER_THAN);
        KEYWORDS.put(">=", TokenType.GREATER_THAN_EQUALS);
        KEYWORDS.put("<", TokenType.LESS_THAN);
        KEYWORDS.put("<=", TokenType.LESS_THAN_EQUALS);
        KEYWORDS.put("=", TokenType.EQUALS);
        KEYWORDS.put("!=", TokenType.NOT_EQUALS);
        KEYWORDS.put("BY", TokenType.BY);
        KEYWORDS.put("AS", TokenType.AS);
        KEYWORDS.put("AND", TokenType.AND);
        KEYWORDS.put("OR", TokenType.OR);
        KEYWORDS.put("NOT", TokenType.NOT);
        KEYWORDS.put("NULL", TokenType.NULL);
        KEYWORDS.put("UNIQUE", TokenType.UNIQUE);
        KEYWORDS.put("PRIMARY", TokenType.PRIMARY);
        KEYWORDS.put("KEY", TokenType.KEY);
        KEYWORDS.put("FOREIGN", TokenType.FOREIGN);
        KEYWORDS.put("REFERENCES", TokenType.REFERENCES);
        KEYWORDS.put("INTEGER", TokenType.INTEGER);
        KEYWORDS.put("STRING", TokenType.STRING);
        KEYWORDS.put("BOOLEAN", TokenType.BOOLEAN);
        KEYWORDS.put("DOUBLE", TokenType.DOUBLE);
        KEYWORDS.put("READ", TokenType.READ);
        KEYWORDS.put("COMMITTED", TokenType.COMMITTED);
        KEYWORDS.put("UNCOMMITTED", TokenType.UNCOMMITTED);
        KEYWORDS.put("REPEATABLE", TokenType.REPEATABLE);
        KEYWORDS.put("SERIALIZABLE", TokenType.SERIALIZABLE);
        KEYWORDS.put("TRANSACTION", TokenType.TRANSACTION);
        KEYWORDS.put("ISOLATION", TokenType.ISOLATION);
        KEYWORDS.put("LEVEL", TokenType.LEVEL);
        KEYWORDS.put("SET", TokenType.SET);
        KEYWORDS.put("BEGIN", TokenType.BEGIN);
        KEYWORDS.put("COMMIT", TokenType.COMMIT);
        KEYWORDS.put("ROLLBACK", TokenType.ROLLBACK);
        KEYWORDS.put("END", TokenType.END);
        KEYWORDS.put("ABORT", TokenType.ABORT);
    }

    public Lexer(String input) {
        this.input = input;
        this.position = 0;
        this.start = 0;
        this.tokens = new ArrayList<>();
    }

    public List<Token> tokenize() {
        while (!isAtEnd()) {
            start = position;
            scanToken();
        }
        tokens.add(new Token(TokenType.EOF, "", position));
        return tokens;
    }

    private void scanToken() {
        char c = advance();
        switch (c) {
            case '(':
                addToken(TokenType.LEFT_PAREN);
                break;
            case ')':
                addToken(TokenType.RIGHT_PAREN);
                break;
            case ',':
                addToken(TokenType.COMMA);
                break;
            case ';':
                addToken(TokenType.SEMICOLON);
                break;
            case '.':
                addToken(TokenType.DOT);
                break;
            case '+':
                addToken(TokenType.PLUS);
                break;
            case '-':
                addToken(TokenType.MINUS);
                break;
            case '*':
                addToken(TokenType.MULTIPLY);
                break;
            case '/':
                addToken(TokenType.DIVIDE);
                break;
            case '=':
                addToken(TokenType.EQUALS);
                break;
            case '<':
                addToken(match('=') ? TokenType.LESS_THAN_EQUALS : TokenType.LESS_THAN);
                break;
            case '>':
                addToken(match('=') ? TokenType.GREATER_THAN_EQUALS : TokenType.GREATER_THAN);
                break;
            case '!':
                addToken(match('=') ? TokenType.NOT_EQUALS : TokenType.NOT);
                break;
            case '\'':
                string();
                break;
            case ' ':
            case '\r':
            case '\t':
            case '\n':
                // Ignore whitespace
                break;
            default:
                if (isDigit(c)) {
                    number();
                } else if (isAlpha(c)) {
                    identifier();
                } else {
                    throw new IllegalStateException("Unexpected character: " + c);
                }
                break;
        }
    }

    private void string() {
        while (peek() != '\'' && !isAtEnd()) {
            advance();
        }

        if (isAtEnd()) {
            throw new IllegalStateException("Unterminated string.");
        }

        // The closing '.
        advance();

        // Trim the surrounding quotes.
        String value = input.substring(start + 1, position - 1);
        addToken(TokenType.STRING, value);
    }

    private void number() {
        while (isDigit(peek())) advance();

        // Look for a decimal point
        if (peek() == '.' && isDigit(peekNext())) {
            // Consume the "."
            advance();

            while (isDigit(peek())) advance();
        }

        String value = input.substring(start, position);
        if (value.contains(".")) {
            addToken(TokenType.DOUBLE, value);
        } else {
            addToken(TokenType.INTEGER, value);
        }
    }

    private void identifier() {
        while (isAlphaNumeric(peek())) advance();

        String text = input.substring(start, position);
        TokenType type = KEYWORDS.get(text.toUpperCase());
        if (type == null) type = TokenType.IDENTIFIER;
        addToken(type, text);
    }

    private boolean match(char expected) {
        if (isAtEnd()) return false;
        if (input.charAt(position) != expected) return false;

        position++;
        return true;
    }

    private char peek() {
        if (isAtEnd()) return '\0';
        return input.charAt(position);
    }

    private char peekNext() {
        if (position + 1 >= input.length()) return '\0';
        return input.charAt(position + 1);
    }

    private boolean isAlpha(char c) {
        return (c >= 'a' && c <= 'z') ||
               (c >= 'A' && c <= 'Z') ||
                c == '_';
    }

    private boolean isAlphaNumeric(char c) {
        return isAlpha(c) || isDigit(c);
    }

    private boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private char advance() {
        return input.charAt(position++);
    }

    private void addToken(TokenType type) {
        addToken(type, input.substring(start, position));
    }

    private void addToken(TokenType type, String value) {
        tokens.add(new Token(type, value, start));
    }

    private boolean isAtEnd() {
        return position >= input.length();
    }
} 