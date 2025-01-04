package com.easydb.sql;

import com.easydb.storage.InMemoryStorage;
import com.easydb.storage.Tuple;
import com.easydb.storage.TupleId;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexType;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.sql.parser.SqlParserFactory;
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
    private SqlParserFactory parserFactory;

    @BeforeEach
    void setUp() {
        storage = new InMemoryStorage();
        sqlEngine = new DefaultSqlEngine(storage);
        parser = new InsertParser(storage);
        parserFactory = new SqlParserFactory(storage);
        String createTable = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name STRING NOT NULL,
                age INTEGER
            )""";
        
        String createIndex = "CREATE INDEX idx_id ON users (id)";
        
        SqlCommand tableCommand = parserFactory.parse(createTable);
        sqlEngine.execute(tableCommand).join();
            
        SqlCommand indexCommand = parserFactory.parse(createIndex);
        sqlEngine.execute(indexCommand).join();
    }

    @Test
    void testSimpleInsert() {
        // Insert data
        String insertSql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 30)";
        SqlCommand command = parserFactory.parse(insertSql);
        
        CompletableFuture<Object> result = sqlEngine.execute(command);
        assertNotNull(result);
        assertEquals(1, result.join());

        String selectSql = "SELECT * FROM users WHERE id = 1";
        ResultSet resultSet = sqlEngine.executeQuery(selectSql).join();
        assertNotNull(resultSet);
        assertEquals(1, resultSet.getRowCount());

        String selectSql2 = "SELECT * FROM users WHERE name = 'John'";
        resultSet = sqlEngine.executeQuery(selectSql2).join();
        assertNotNull(resultSet);
        assertEquals(1, resultSet.getRowCount());
    }


    @Test
    void testInsertWithNullValue() {
        String sql = "INSERT INTO users (id, name, age) VALUES (2, 'John', null)";
        SqlCommand command = parser.parse(sql);
        
        CompletableFuture<Object> result = sqlEngine.execute(command);
        assertNotNull(result);
        assertEquals(1, result.join());
    }

    @Test
    void testInvalidInsert() {
        String sql = "INSERT INTO nonexistent (id) VALUES (1)";
        
        assertThrows(IllegalArgumentException.class, () -> parser.parse(sql));
    }

    @Test
    void testInvalidSyntax() {
        String sql = "INSERT INTO users VALUES (1)"; // Missing column list
        assertThrows(IllegalArgumentException.class, () -> parser.parse(sql));
    }
} 