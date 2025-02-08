package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SelectParserTest {
    @Test
    void testSimpleSelect() {
        String sql = "SELECT * FROM users;";
        Lexer lexer = new Lexer(sql);
        SelectParser parser = new SelectParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        assertEquals(ParseTreeType.SELECT_STATEMENT, tree.getType());
        assertEquals(2, tree.getChildCount());

        // Check SELECT list
        ParseTree selectList = tree.getChild(0);
        assertEquals(ParseTreeType.SELECT_LIST, selectList.getType());
        assertEquals(1, selectList.getChildCount());
        assertEquals(ParseTreeType.ASTERISK, selectList.getChild(0).getType());

        // Check FROM clause
        ParseTree fromClause = tree.getChild(1);
        assertEquals(ParseTreeType.FROM_CLAUSE, fromClause.getType());
        assertEquals(1, fromClause.getChildCount());
        ParseTree tableRef = fromClause.getChild(0);
        assertEquals(ParseTreeType.TABLE_REF, tableRef.getType());
        assertEquals("users", tableRef.getValue());
    }

    @Test
    void testSelectWithColumns() {
        String sql = "SELECT id, name, age FROM users;";
        Lexer lexer = new Lexer(sql);
        SelectParser parser = new SelectParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        ParseTree selectList = tree.getChild(0);
        assertEquals(3, selectList.getChildCount());
        assertEquals(ParseTreeType.COLUMN_REF, selectList.getChild(0).getType());
        assertEquals("id", selectList.getChild(0).getValue());
        assertEquals(ParseTreeType.COLUMN_REF, selectList.getChild(1).getType());
        assertEquals("name", selectList.getChild(1).getValue());
        assertEquals(ParseTreeType.COLUMN_REF, selectList.getChild(2).getType());
        assertEquals("age", selectList.getChild(2).getValue());
    }

    @Test
    void testSelectWithWhere() {
        String sql = "SELECT * FROM users WHERE age >= 18 AND city = 'New York';";
        Lexer lexer = new Lexer(sql);
        SelectParser parser = new SelectParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        assertEquals(3, tree.getChildCount());
        ParseTree whereClause = tree.getChild(2);
        assertEquals(ParseTreeType.WHERE_CLAUSE, whereClause.getType());
        
        // Check the AND expression
        ParseTree andExpr = whereClause.getChild(0);
        assertEquals(ParseTreeType.BINARY_EXPR, andExpr.getType());
        assertEquals(ParseTreeType.AND_OPERATOR, andExpr.getChild(1).getType());
    }

    @Test
    void testSelectWithOrderBy() {
        String sql = "SELECT * FROM users ORDER BY name, age;";
        Lexer lexer = new Lexer(sql);
        SelectParser parser = new SelectParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        assertEquals(3, tree.getChildCount());
        ParseTree orderByClause = tree.getChild(2);
        assertEquals(ParseTreeType.ORDER_BY_CLAUSE, orderByClause.getType());
        assertEquals(2, orderByClause.getChildCount());
        assertEquals("name", orderByClause.getChild(0).getValue());
        assertEquals("age", orderByClause.getChild(1).getValue());
    }

    @Test
    void testSelectWithGroupBy() {
        String sql = "SELECT department, date FROM employees GROUP BY department;";
        Lexer lexer = new Lexer(sql);
        SelectParser parser = new SelectParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        assertEquals(3, tree.getChildCount());
        ParseTree groupByClause = tree.getChild(2);
        assertEquals(ParseTreeType.GROUP_BY_CLAUSE, groupByClause.getType());
        assertEquals(1, groupByClause.getChildCount());
        assertEquals("department", groupByClause.getChild(0).getValue());
    }

    @Test
    void testSelectWithAlias() {
        String sql = "SELECT name AS employee_name FROM users u;";
        Lexer lexer = new Lexer(sql);
        SelectParser parser = new SelectParser(lexer.tokenize());
        ParseTree tree = parser.parse();

        ParseTree selectList = tree.getChild(0);
        ParseTree alias = selectList.getChild(0);
        assertEquals(ParseTreeType.ALIAS, alias.getType());
        assertEquals("employee_name", alias.getValue());
        assertEquals(ParseTreeType.COLUMN_REF, alias.getChild(0).getType());
        assertEquals("name", alias.getChild(0).getValue());

        ParseTree fromClause = tree.getChild(1);
        ParseTree tableAlias = fromClause.getChild(0);
        assertEquals(ParseTreeType.ALIAS, tableAlias.getType());
        assertEquals("u", tableAlias.getValue());
    }

    @Test
    void testInvalidSelect() {
        String sql = "SELECT FROM users;"; // Missing columns
        Lexer lexer = new Lexer(sql);
        SelectParser parser = new SelectParser(lexer.tokenize());
        assertThrows(ParseException.class, parser::parse);
    }
} 