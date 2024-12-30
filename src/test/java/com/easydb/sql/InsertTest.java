package com.easydb.sql;

import com.easydb.storage.InMemoryStorage;
import com.easydb.storage.Tuple;
import com.easydb.storage.TupleId;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.core.Column;
import com.easydb.core.DataType;
import com.easydb.sql.command.SqlCommand;
import com.easydb.sql.parser.InsertParser;
import com.easydb.sql.result.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class InsertTest {
    private SqlEngine sqlEngine;
    private InMemoryStorage storage;
    private InsertParser parser;

    @BeforeEach
    void setUp() {
        storage = new InMemoryStorage();
        sqlEngine = new DefaultSqlEngine(storage);
        parser = new InsertParser();

        // Create test table
        List<Column> columns = Arrays.asList(
            new Column("id", DataType.INTEGER, true, true, false, null, 0),
            new Column("name", DataType.STRING, false, false, false, null, 1),
            new Column("age", DataType.INTEGER, true, false, false, null, 2)
        );
        TableMetadata table = new TableMetadata("users", columns);
        storage.createTable(table).join();
    }

    @Test
    void testSimpleInsert() {
        // Insert data
        String insertSql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 30)";
        SqlCommand command = parser.parse(insertSql);
        
        CompletableFuture<Object> result = sqlEngine.execute(command);
        assertNotNull(result);
        assertEquals(1, result.join());
    }

    @Test
    void testInsertWithNullValue() {
        String sql = "INSERT INTO users (id, name, age) VALUES (2, 'Jane', null)";
        SqlCommand command = parser.parse(sql);
        
        CompletableFuture<Object> result = sqlEngine.execute(command);
        assertNotNull(result);
        assertEquals(1, result.join());
    }

    @Test
    void testInvalidInsert() {
        String sql = "INSERT INTO nonexistent (id) VALUES (1)";
        SqlCommand command = parser.parse(sql);
        
        assertThrows(IllegalArgumentException.class, () -> sqlEngine.execute(command));
    }

    @Test
    void testInvalidSyntax() {
        String sql = "INSERT INTO users VALUES (1)"; // Missing column list
        assertThrows(IllegalArgumentException.class, () -> parser.parse(sql));
    }
} 