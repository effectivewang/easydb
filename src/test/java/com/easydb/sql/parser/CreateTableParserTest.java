package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CreateTableParserTest {
    @Test
    void testSimpleCreateTable() {
        String sql = "CREATE TABLE users (id INTEGER, name STRING);";
        Lexer lexer = new Lexer(sql);
        CreateTableParser parser = new CreateTableParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        assertEquals(ParseTreeType.CREATE_TABLE_STATEMENT, tree.getType());
        assertEquals(2, tree.getChildCount());

        // Check table name
        ParseTree tableRef = tree.getChild(0);
        assertEquals(ParseTreeType.TABLE_REF, tableRef.getType());
        assertEquals("users", tableRef.getValue());

        // Check column definitions
        ParseTree columnList = tree.getChild(1);
        assertEquals(ParseTreeType.COLUMN_LIST, columnList.getType());
        assertEquals(2, columnList.getChildCount());

        // Check first column
        ParseTree firstColumn = columnList.getChild(0);
        assertEquals(ParseTreeType.COLUMN_REF, firstColumn.getType());
        assertEquals("id", firstColumn.getValue());
        assertEquals(ParseTreeType.INTEGER_TYPE, firstColumn.getChild(0).getType());

        // Check second column
        ParseTree secondColumn = columnList.getChild(1);
        assertEquals(ParseTreeType.COLUMN_REF, secondColumn.getType());
        assertEquals("name", secondColumn.getValue());
        assertEquals(ParseTreeType.STRING_TYPE, secondColumn.getChild(0).getType());
    }

    @Test
    void testCreateTableWithConstraints() {
        String sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING NOT NULL);";
        Lexer lexer = new Lexer(sql);
        CreateTableParser parser = new CreateTableParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        ParseTree columnList = tree.getChild(1);
        
        // Check first column with PRIMARY KEY
        ParseTree firstColumn = columnList.getChild(0);
        assertEquals(2, firstColumn.getChildCount());
        assertEquals(ParseTreeType.PRIMARY_KEY_CONSTRAINT, firstColumn.getChild(1).getType());

        // Check second column with NOT NULL
        ParseTree secondColumn = columnList.getChild(1);
        assertEquals(2, secondColumn.getChildCount());
        assertEquals(ParseTreeType.NOT_NULL_CONSTRAINT, secondColumn.getChild(1).getType());
    }

    @Test
    void testCreateTableWithForeignKey() {
        String sql = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER FOREIGN KEY REFERENCES users(id));";
        Lexer lexer = new Lexer(sql);
        CreateTableParser parser = new CreateTableParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        ParseTree columnList = tree.getChild(1);
        ParseTree userIdColumn = columnList.getChild(1);
        
        // Check FOREIGN KEY constraint
        ParseTree foreignKey = userIdColumn.getChild(1);
        assertEquals(ParseTreeType.FOREIGN_KEY_CONSTRAINT, foreignKey.getType());
        assertEquals(2, foreignKey.getChildCount());
        assertEquals("users", foreignKey.getChild(0).getValue());
        assertEquals("id", foreignKey.getChild(1).getValue());
    }

    @Test
    void testCreateTableWithMultipleConstraints() {
        String sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, email STRING NOT NULL UNIQUE);";
        Lexer lexer = new Lexer(sql);
        CreateTableParser parser = new CreateTableParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        ParseTree columnList = tree.getChild(1);
        ParseTree emailColumn = columnList.getChild(1);
        
        // Check both NOT NULL and UNIQUE constraints
        assertEquals(3, emailColumn.getChildCount());
        assertEquals(ParseTreeType.NOT_NULL_CONSTRAINT, emailColumn.getChild(1).getType());
        assertEquals(ParseTreeType.UNIQUE_CONSTRAINT, emailColumn.getChild(2).getType());
    }

    @Test
    void testInvalidCreateTable() {
        String sql = "CREATE TABLE users;"; // Missing column definitions
        Lexer lexer = new Lexer(sql);
        CreateTableParser parser = new CreateTableParser(lexer.tokenize());
        assertThrows(ParseException.class, parser::parse);
    }

    @Test
    void testInvalidColumnDefinition() {
        String sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name);"; // Missing data type
        Lexer lexer = new Lexer(sql);
        CreateTableParser parser = new CreateTableParser(lexer.tokenize());
        assertThrows(ParseException.class, parser::parse);
    }

    @Test
    void testInvalidConstraint() {
        String sql = "CREATE TABLE users (id INTEGER PRIMARY, name STRING);"; // Incomplete PRIMARY KEY
        Lexer lexer = new Lexer(sql);
        CreateTableParser parser = new CreateTableParser(lexer.tokenize());
        assertThrows(ParseException.class, parser::parse);
    }
} 