package com.easydb.sql.result;

import com.easydb.core.DataType;
import com.easydb.core.Column;
import org.junit.jupiter.api.Test;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ResultSetSerializationTest {
    @Test
    void testNullValues() {
        ResultSet.Builder builder = new ResultSet.Builder();
        builder.addColumn(new Column("name", DataType.STRING));
        
        Map<String, Object> row = new HashMap<>();
        row.put("name", null);
        builder.addRow(row);
        
        ResultSet resultSet = builder.build();
        assertNotNull(resultSet);
        assertEquals(1, resultSet.getRowCount());
        assertNull(resultSet.getRows().get(0).getString("name"));
    }

    @Test
    void testEscapeCharacters() {
        ResultSet.Builder builder = new ResultSet.Builder();
        builder.addColumn(new Column("text", DataType.STRING));
        
        Map<String, Object> row = new HashMap<>();
        row.put("text", "Line 1\nLine 2\tTabbed\r\nWindows");
        builder.addRow(row);
        
        ResultSet resultSet = builder.build();
        assertNotNull(resultSet);
        assertEquals(1, resultSet.getRowCount());
        assertEquals("Line 1\nLine 2\tTabbed\r\nWindows", resultSet.getRows().get(0).getString("text"));
    }

    @Test
    void testMultipleColumns() {
        ResultSet.Builder builder = new ResultSet.Builder();
        builder.addColumn(new Column("name", DataType.STRING))
               .addColumn(new Column("age", DataType.INTEGER))
               .addColumn(new Column("active", DataType.BOOLEAN));
        
        Map<String, Object> row = new HashMap<>();
        row.put("name", "John");
        row.put("age", 30);
        row.put("active", true);
        builder.addRow(row);
        
        ResultSet resultSet = builder.build();
        assertNotNull(resultSet);
        assertEquals(1, resultSet.getRowCount());
        
        ResultSet.Row resultRow = resultSet.getRows().get(0);
        assertEquals("John", resultRow.getString("name"));
        assertEquals(30, resultRow.getInteger("age"));
        assertTrue(resultRow.getBoolean("active"));
    }
} 