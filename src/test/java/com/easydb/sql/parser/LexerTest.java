package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class LexerTest {
    @Test
    void testSimpleSelect() {
        String sql = "SELECT * FROM users;";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();
        
        assertEquals(6, tokens.size());
        assertEquals(TokenType.SELECT, tokens.get(0).type());
        assertEquals(TokenType.MULTIPLY, tokens.get(1).type());
        assertEquals(TokenType.FROM, tokens.get(2).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(3).type());
        assertEquals("users", tokens.get(3).value());
        assertEquals(TokenType.SEMICOLON, tokens.get(4).type());
        assertEquals(TokenType.EOF, tokens.get(5).type());
    }

    @Test
    void testInsertStatement() {
        String sql = "INSERT INTO users VALUES ('John', 25);";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        assertEquals(10, tokens.size());
        assertEquals(TokenType.INSERT, tokens.get(0).type());
        assertEquals(TokenType.INTO, tokens.get(1).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(2).type());
        assertEquals("users", tokens.get(2).value());
        assertEquals(TokenType.VALUES, tokens.get(3).type());
        assertEquals(TokenType.LEFT_PAREN, tokens.get(4).type());
        assertEquals(TokenType.STRING_LITERAL, tokens.get(5).type());
        assertEquals("John", tokens.get(5).value());
        assertEquals(TokenType.COMMA, tokens.get(6).type());
        assertEquals(TokenType.NUMBER_LITERAL, tokens.get(7).type());
        assertEquals("25", tokens.get(7).value());
        assertEquals(TokenType.RIGHT_PAREN, tokens.get(8).type());
        assertEquals(TokenType.SEMICOLON, tokens.get(9).type());
    }

    @Test
    void testWhereClause() {
        String sql = "SELECT name FROM users WHERE age >= 18 AND city = 'New York';";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        assertTrue(tokens.stream().anyMatch(t -> t.type() == TokenType.WHERE));
        assertTrue(tokens.stream().anyMatch(t -> t.type() == TokenType.GREATER_THAN_EQUALS));
        assertTrue(tokens.stream().anyMatch(t -> t.type() == TokenType.AND));
        assertTrue(tokens.stream().anyMatch(t -> t.type() == TokenType.EQUALS));
        assertTrue(tokens.stream().anyMatch(t -> t.value().equals("New York")));
    }

    @Test
    void testInvalidCharacter() {
        String sql = "SELECT @ FROM users;";
        Lexer lexer = new Lexer(sql);
        assertThrows(IllegalStateException.class, lexer::tokenize);
    }

    @Test
    void testUnterminatedString() {
        String sql = "SELECT * FROM users WHERE name = 'John;";
        Lexer lexer = new Lexer(sql);
        assertThrows(IllegalStateException.class, lexer::tokenize);
    }
} 