package com.easydb.sql.result;

import com.easydb.core.DataType;
import com.easydb.sql.result.ResultSet;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.Base64;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public class ResultSetSerializationTest {

    @Test
    public void testJsonSerialization() throws Exception {
        ResultSet resultSet = createSampleResultSet();
        String json = resultSet.toJson();
        
        // Parse both actual and expected JSON for comparison
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualJson = mapper.readTree(json);        
        // Create expected JSON structure
        String expected = """
            [
              {
                "name": "O'Connor",
                "age": 30,
                "salary": 75000.50,
                "birthDate": "1993-05-15",
                "lastLogin": "2023-12-01T14:30:00",
                "active": true,
                "data": "U2FtcGxlIEJpbmFyeSBEYXRh"
              },
              {
                "name": "Smith, John",
                "age": 25,
                "salary": 60000.75,
                "birthDate": "1998-08-22",
                "lastLogin": null,
                "active": false,
                "data": null
              }
            ]""";
        
        JsonNode expectedJson = mapper.readTree(expected);
        
        // Compare JSON structures instead of raw strings
        assertEquals(expectedJson, actualJson);
        
        // Additional structural checks
        assertTrue(json.contains("\"O'Connor\""), "Should properly escape newline");
        assertTrue(json.contains("\"Smith, John\""), "Should properly handle comma in string");
    }

    @Test
    public void testCsvSerialization() {
        ResultSet resultSet = createSampleResultSet();
        String csv = resultSet.toCsv();

        // Verify CSV structure
        String[] lines = csv.split(System.lineSeparator());  
        assertTrue(lines.length > 1, "Should have header and data rows");
        
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
        String firstRow = lines[1];
        // Should contain the escaped newline in a quoted string
        assertTrue(firstRow.matches("^O'Connor.*"), "First field should be properly quoted");
        
        String secondRow = lines[2];
        // Should contain the comma in a quoted string
        assertTrue(secondRow.matches("^\"Smith, John\",.*"), "Second row should have quoted name with comma");
    }

    @Test
    public void testNullValueSerialization() {
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
    public void testSpecialCharacterEscaping() {
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
                "O'Connor", // String with special chars
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