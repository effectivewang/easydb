package com.easydb.sql.parser;

import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SqlParserFactoryTest {
    private SqlParserFactory factory;

    @BeforeEach
    void setUp() {
        factory = new SqlParserFactory();
    }

    @Test
    void testSelectStatement() {
        String sql = "SELECT * FROM users;";
        ParseTree tree = factory.parse(sql);
        assertEquals(ParseTreeType.SELECT_STATEMENT, tree.getType());
    }

    @Test
    void testInsertStatement() {
        String sql = "INSERT INTO users VALUES ('John', 25);";
        ParseTree tree = factory.parse(sql);
        assertEquals(ParseTreeType.INSERT_STATEMENT, tree.getType());
    }

    @Test
    void testCreateTableStatement() {
        String sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING);";
        ParseTree tree = factory.parse(sql);
        assertEquals(ParseTreeType.CREATE_TABLE_STATEMENT, tree.getType());
    }

    @Test
    void testUnsupportedStatement() {
        String sql = "DROP TABLE users;";
        assertThrows(ParseException.class, () -> factory.parse(sql));
    }

    @Test
    void testInvalidSql() {
        String sql = "INVALID SQL";
        assertThrows(ParseException.class, () -> factory.parse(sql));
    }

    @Test
    void testEmptySql() {
        String sql = "";
        assertThrows(ParseException.class, () -> factory.parse(sql));
    }

    @Test
    void testComplexSelect() {
        String sql = "SELECT name, age FROM users WHERE age >= 18 ORDER BY name;";
        ParseTree tree = factory.parse(sql);
        assertEquals(ParseTreeType.SELECT_STATEMENT, tree.getType());
        assertTrue(tree.getChildCount() > 2); // Should have SELECT list, FROM clause, WHERE clause, and ORDER BY clause
    }

    @Test
    void testComplexInsert() {
        String sql = "INSERT INTO users (name, age, email) VALUES ('John', 25, 'john@example.com');";
        ParseTree tree = factory.parse(sql);
        assertEquals(ParseTreeType.INSERT_STATEMENT, tree.getType());
        assertEquals(3, tree.getChildCount()); // Should have table ref, column list, and values
    }

    @Test
    void testComplexCreateTable() {
        String sql = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER FOREIGN KEY REFERENCES users(id), total DOUBLE NOT NULL, status STRING);";
        ParseTree tree = factory.parse(sql);
        assertEquals(ParseTreeType.CREATE_TABLE_STATEMENT, tree.getType());
        
        ParseTree columnList = tree.getChild(1);
        assertEquals(4, columnList.getChildCount()); // Should have 4 columns
    }
} 