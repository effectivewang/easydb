package com.easydb.sql.result;

import com.easydb.core.DataType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.Base64;

class ResultSetSerializationTest {

    @Test
    void testJsonSerialization() {
        ResultSet resultSet = createSampleResultSet();
        String json = resultSet.toJson();

        // Verify JSON structure
        assertTrue(json.startsWith("["));
        assertTrue(json.endsWith("]"));
        assertTrue(json.contains("\"name\""));
        assertTrue(json.contains("\"age\""));
        assertTrue(json.contains("\"salary\""));
        assertTrue(json.contains("\"birthDate\""));
        assertTrue(json.contains("\"lastLogin\""));
        assertTrue(json.contains("\"active\""));
        assertTrue(json.contains("\"data\""));

        // Verify JSON escaping
        assertTrue(json.contains("O\\'Connor")); // Name with quote
        assertTrue(json.contains("\\n")); // Newline in text
    }

    @Test
    void testCsvSerialization() {
        ResultSet resultSet = createSampleResultSet();
        String csv = resultSet.toCsv();

        // Verify CSV structure
        String[] lines = csv.split("\n");
        assertTrue(lines.length > 1); // Header + data rows
        
        // Verify header
        String header = lines[0];
        assertTrue(header.contains("name"));
        assertTrue(header.contains("age"));
        assertTrue(header.contains("salary"));
        assertTrue(header.contains("birthDate"));
        assertTrue(header.contains("lastLogin"));
        assertTrue(header.contains("active"));
        assertTrue(header.contains("data"));

        // Verify data escaping
        String dataRow = lines[1];
        assertTrue(dataRow.contains("\"\"")); // Escaped quotes
        assertTrue(dataRow.matches(".*\"[^\"]*,[^\"]*\".*")); // Quoted value containing comma
    }

    @Test
    void testNullValueSerialization() {
        ResultSet.Builder builder = new ResultSet.Builder()
            .addColumn("name", DataType.STRING, true)
            .addColumn("age", DataType.INTEGER, true);

        builder.addRow("John", null);
        ResultSet resultSet = builder.build();

        // Test JSON null
        String json = resultSet.toJson();
        assertTrue(json.contains("null"));

        // Test CSV null
        String csv = resultSet.toCsv();
        assertTrue(csv.contains("John,"));
    }

    @Test
    void testSpecialCharacterEscaping() {
        ResultSet.Builder builder = new ResultSet.Builder()
            .addColumn("text", DataType.STRING, true);

        builder.addRow("Line 1\nLine 2\tTabbed\r\nWindows");
        ResultSet resultSet = builder.build();

        // Test JSON escaping
        String json = resultSet.toJson();
        assertTrue(json.contains("\\n"));
        assertTrue(json.contains("\\t"));
        assertTrue(json.contains("\\r"));

        // Test CSV escaping
        String csv = resultSet.toCsv();
        assertTrue(csv.contains("\"Line 1\nLine 2\tTabbed\r\nWindows\""));
    }

    private ResultSet createSampleResultSet() {
        byte[] binaryData = "Sample Binary Data".getBytes();
        
        return new ResultSet.Builder()
            .addColumn("name", DataType.STRING, false)
            .addColumn("age", DataType.INTEGER, false)
            .addColumn("salary", DataType.DECIMAL, true)
            .addColumn("birthDate", DataType.DATE, false)
            .addColumn("lastLogin", DataType.DATETIME, true)
            .addColumn("active", DataType.BOOLEAN, false)
            .addColumn("data", DataType.BYTES, true)
            .addRow(
                "O'Connor\nNext Line", // String with special chars
                30,
                new BigDecimal("75000.50"),
                LocalDate.of(1993, 5, 15),
                LocalDateTime.of(2023, 12, 1, 14, 30, 0),
                true,
                binaryData
            )
            .addRow(
                "Smith, John", // String with comma
                25,
                new BigDecimal("60000.75"),
                LocalDate.of(1998, 8, 22),
                null,
                false,
                null
            )
            .build();
    }
} 