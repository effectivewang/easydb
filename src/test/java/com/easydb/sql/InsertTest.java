package com.easydb.sql;

import com.easydb.storage.InMemoryStorage;
import com.easydb.storage.Tuple;
import com.easydb.storage.TupleId;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.index.IndexType;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.storage.transaction.TransactionManager;
import com.easydb.sql.parser.SqlParserFactory;
import com.easydb.core.Column;
import com.easydb.core.DataType;
import com.easydb.sql.parser.InsertParser;
import com.easydb.sql.result.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class InsertTest {
    private SqlEngine sqlEngine;
    private TransactionManager transactionManager;  
    private InMemoryStorage storage;
    private InsertParser parser;
    private SqlParserFactory parserFactory;
    private ExecutionContext executionContext;

    @BeforeEach
    void setUp() {
        transactionManager = new TransactionManager();
        storage = new InMemoryStorage(transactionManager);
        executionContext = new ExecutionContext(transactionManager);
        sqlEngine = new DefaultSqlEngine(storage);
        parserFactory = new SqlParserFactory();
        String createTable = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name STRING NOT NULL,
                age INTEGER
            )""";
        
        String createIndex = "CREATE INDEX idx_id ON users (id)";
        
        sqlEngine.executeUpdate(createTable);
        sqlEngine.executeUpdate(createIndex);
    }

    @Test
    void testSimpleInsert() {
        // Insert data
        String insertSql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 30)";
        
        Integer result = sqlEngine.executeUpdate(insertSql, executionContext);
        assertNotNull(result);
        assertEquals(1, result);

        String selectSql = "SELECT * FROM users WHERE id = 1";
        ResultSet resultSet = sqlEngine.executeQuery(selectSql, executionContext);
        assertNotNull(resultSet);
        assertEquals(1, resultSet.getRowCount());

        String selectSql2 = "SELECT id, age FROM users WHERE name = 'John'";
        resultSet = sqlEngine.executeQuery(selectSql2, executionContext);
        assertNotNull(resultSet);
        assertEquals(1, resultSet.getRowCount());
        List<ResultSet.Row> rows = resultSet.getRows();
        assertEquals(1, rows.size());
        ResultSet.Row row = rows.get(0);
        assertEquals(1, row.getInteger("id"));
        assertEquals(30, row.getInteger("age"));
    }


    @Test
    void testInsertWithNullValue() {
        String sql = "INSERT INTO users (id, name, age) VALUES (2, 'John', null)";
        
        Integer result = sqlEngine.executeUpdate(sql, executionContext);
        assertNotNull(result);
        assertEquals(1, result);
    }

    @Test
    void testInvalidInsert() {
        String sql = "INSERT INTO nonexistent (id) VALUES (1)";
        
        assertThrows(IllegalArgumentException.class, () -> sqlEngine.executeUpdate(sql, executionContext));
    }

    @Test
    void testInvalidSyntax() {
        String sql = "INSERT INTO users VALUES (1)"; // Missing column list
        assertThrows(IllegalArgumentException.class, () -> sqlEngine.executeUpdate(sql, executionContext));
    }
} 