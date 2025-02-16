package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class InsertParserTest {
    @Test
    void testSimpleInsert() {
        String sql = "INSERT INTO users VALUES ('John', 25);";
        Lexer lexer = new Lexer(sql);
        InsertParser parser = new InsertParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        assertEquals(ParseTreeType.INSERT_STATEMENT, tree.getType());
        assertEquals(2, tree.getChildCount());

        // Check table reference
        ParseTree tableRef = tree.getChild(0);
        assertEquals(ParseTreeType.TABLE_REF, tableRef.getType());
        assertEquals("users", tableRef.getValue());

        // Check values
        ParseTree values = tree.getChild(1);
        assertEquals(ParseTreeType.VALUES_CLAUSE, values.getType());
        assertEquals(1, values.getChildCount());

        ParseTree valueList = values.getChild(0);
        assertEquals(ParseTreeType.LIST, valueList.getType());
        assertEquals(2, valueList.getChildCount());

        ParseTree firstValue = valueList.getChild(0);
        assertEquals(ParseTreeType.STRING_TYPE, firstValue.getType());
        assertEquals("John", firstValue.getValue());

        ParseTree secondValue = valueList.getChild(1);
        assertEquals(ParseTreeType.INTEGER_TYPE, secondValue.getType());
        assertEquals("25", secondValue.getValue());
    }

    @Test
    void testInsertWithColumns() {
        String sql = "INSERT INTO users (name, age) VALUES ('John', 25);";
        Lexer lexer = new Lexer(sql);
        InsertParser parser = new InsertParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        assertEquals(3, tree.getChildCount());

        // Check column list
        ParseTree columns = tree.getChild(1);
        assertEquals(ParseTreeType.LIST, columns.getType());
        assertEquals(2, columns.getChildCount());
        assertEquals("name", columns.getChild(0).getValue());
        assertEquals("age", columns.getChild(1).getValue());

        // Check values
        ParseTree values = tree.getChild(2);
        assertEquals(ParseTreeType.VALUES_CLAUSE, values.getType());
        assertEquals(1, values.getChildCount());
    }

    @Test
    void testInsertMultipleRows() {
        String sql = "INSERT INTO users VALUES ('John', 25), ('Jane', 30);";
        Lexer lexer = new Lexer(sql);
        InsertParser parser = new InsertParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        ParseTree values = tree.getChild(1);
        assertEquals(2, values.getChildCount());

        // Check first row
        ParseTree firstRow = values.getChild(0);
        assertEquals(2, firstRow.getChildCount());
        assertEquals("John", firstRow.getChild(0).getValue());
        assertEquals("25", firstRow.getChild(1).getValue());

        // Check second row
        ParseTree secondRow = values.getChild(1);
        assertEquals(2, secondRow.getChildCount());
        assertEquals("Jane", secondRow.getChild(0).getValue());
        assertEquals("30", secondRow.getChild(1).getValue());
    }

    @Test
    void testInsertWithNull() {
        String sql = "INSERT INTO users (name, age) VALUES ('John', NULL);";
        Lexer lexer = new Lexer(sql);
        InsertParser parser = new InsertParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        ParseTree values = tree.getChild(2);
        ParseTree valueList = values.getChild(0);
        assertEquals(2, valueList.getChildCount());
        assertEquals(ParseTreeType.NULL_TYPE, valueList.getChild(1).getType());
    }

    @Test
    void testInvalidInsert() {
        String sql = "INSERT INTO users VALUES;"; // Missing values
        Lexer lexer = new Lexer(sql);
        InsertParser parser = new InsertParser(lexer.tokenize());
        assertThrows(ParseException.class, parser::parse);
    }

    @Test
    void testMismatchedParentheses() {
        String sql = "INSERT INTO users (name, age VALUES ('John', 25);"; // Missing closing parenthesis
        Lexer lexer = new Lexer(sql);
        InsertParser parser = new InsertParser(lexer.tokenize());
        assertThrows(ParseException.class, parser::parse);
    }
} 