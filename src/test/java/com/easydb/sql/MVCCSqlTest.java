package com.easydb.sql;

import org.junit.jupiter.api.*;

import com.easydb.sql.parser.SqlParserFactory;
import com.easydb.storage.transaction.Transaction;
import com.easydb.storage.transaction.IsolationLevel;
import com.easydb.storage.Storage;
import com.easydb.storage.InMemoryStorage;
import com.easydb.sql.DefaultSqlEngine;
import com.easydb.storage.transaction.TransactionManager;
import com.easydb.storage.*;
import com.easydb.sql.result.ResultSet;
import java.util.concurrent.atomic.AtomicLong;
import com.easydb.sql.executor.ExecutionContext;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;


class MVCCSqlTest {
    private DefaultSqlEngine sqlEngine;
    private SqlParserFactory sqlParserFactory;
    private InMemoryStorage storage;   
    private TransactionManager transactionManager;
    private AtomicLong globalTs;

    @BeforeEach
    void setUp() {
        transactionManager = new TransactionManager();
        storage = new InMemoryStorage(transactionManager);
        sqlEngine = new DefaultSqlEngine(storage);
        setupTestData(transactionManager);
    }

    @Test
    void testReadCommitted() throws ExecutionException, InterruptedException {
        // Start transaction 1 with READ COMMITTED isolation
        ExecutionContext executionContext = new ExecutionContext(transactionManager);
        executionContext.beginTransaction();

        // Start transaction 2
        ExecutionContext executionContext2 = new ExecutionContext(transactionManager);
        executionContext2.beginTransaction();

        // Transaction 1 reads data
        ResultSet result1 = sqlEngine.executeQuery(
            "SELECT balance FROM accounts WHERE id = 1", executionContext);
        System.out.println(result1.toString());
        int initialBalance = result1.getRows().get(0).getInteger("balance");

        // Transaction 2 updates data
        sqlEngine.executeUpdate(
            "UPDATE accounts SET balance = balance + 100 WHERE id = 1", executionContext2);
        sqlEngine.commitTransaction(executionContext2);

        // Transaction 1 reads again - should see updated data  
        result1 = sqlEngine.executeQuery(
            "SELECT balance FROM accounts WHERE id = 1", executionContext);
        int updatedBalance = (Integer) result1.getRows().get(0).getInteger("balance");

        assertEquals(initialBalance + 100, updatedBalance);
        sqlEngine.commitTransaction(executionContext); 
    }

    /*
    @Test   
    void testRepeatableRead() throws ExecutionException, InterruptedException {
        // Start transaction 1 with REPEATABLE READ isolation
        Transaction tx1 = sqlEngine.beginTransaction().get();
        sqlEngine.executeUpdate("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").get();

        // Start transaction 2
        Transaction tx2 = sqlEngine.beginTransaction().get();
        sqlEngine.executeUpdate("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").get();

        // Transaction 1 reads data
        List<Map<String, Object>> result1 = sqlEngine.executeQuery(
            "SELECT balance FROM accounts WHERE id = 1").get();
        int initialBalance = (Integer) result1.get(0).get("balance");

        // Transaction 2 updates and commits
        sqlEngine.executeUpdate(
            "UPDATE accounts SET balance = balance + 100 WHERE id = 1").get();
        tx2.commit().get();

        // Transaction 1 reads again - should see same data (repeatable read)
        result1 = sqlEngine.executeQuery(
            "SELECT balance FROM accounts WHERE id = 1").get();
        int sameBalance = (Integer) result1.get(0).get("balance");

        assertEquals(initialBalance, sameBalance);
        tx1.commit().get();
    }

    @Test
    void testSerializable() throws ExecutionException, InterruptedException {
        // Start transaction 1 with SERIALIZABLE isolation
        Transaction tx1 = sqlEngine.beginTransaction().get();
        sqlEngine.executeUpdate("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").get();

        // Start transaction 2
        Transaction tx2 = sqlEngine.beginTransaction().get();
        sqlEngine.executeUpdate("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").get();

        // Both transactions try to update the same row
        CompletableFuture<Integer> update1 = sqlEngine.executeUpdate(
            "UPDATE accounts SET balance = balance + 100 WHERE id = 1");
        CompletableFuture<Integer> update2 = sqlEngine.executeUpdate(
            "UPDATE accounts SET balance = balance + 200 WHERE id = 1");

        // One transaction should succeed, one should fail
        try {
            update1.get();
            tx1.commit().get();
            
            // Second transaction should fail with serialization error
            assertThrows(ExecutionException.class, () -> {
                update2.get();
                tx2.commit().get();
            });
        } catch (ExecutionException e) {
            // If first transaction failed, second should succeed
            update2.get();
            tx2.commit().get();
            assertThrows(ExecutionException.class, () -> tx1.commit().get());
        }
    }

    @Test
    void testPhantomRead() throws ExecutionException, InterruptedException {
        // Start transaction 1 with REPEATABLE READ isolation
        Transaction tx1 = sqlEngine.beginTransaction().get();
        sqlEngine.executeUpdate("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").get();

        // First read
        List<Map<String, Object>> result1 = sqlEngine.executeQuery(
            "SELECT * FROM accounts WHERE balance > 1000").get();
        int initialCount = result1.size();

        // Transaction 2 inserts new row
        Transaction tx2 = sqlEngine.beginTransaction().get();
        sqlEngine.executeUpdate("""
            INSERT INTO accounts (id, user_id, balance) 
            VALUES (4, 4, 1500)
            """).get();
        tx2.commit().get();

        // Second read in transaction 1 - should not see phantom row
        result1 = sqlEngine.executeQuery(
            "SELECT * FROM accounts WHERE balance > 1000").get();
        assertEquals(initialCount, result1.size());
        tx1.commit().get();
    }

    @Test
    void testDirtyRead() throws ExecutionException, InterruptedException {
        // Start transaction 1 with READ UNCOMMITTED isolation
        Transaction tx1 = sqlEngine.beginTransaction().get();
        sqlEngine.executeUpdate("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED").get();

        // Start transaction 2
        Transaction tx2 = sqlEngine.beginTransaction().get();
        
        // Transaction 2 updates but doesn't commit
        sqlEngine.executeUpdate(
            "UPDATE accounts SET balance = balance + 100 WHERE id = 1").get();

        // Transaction 1 reads - might see uncommitted data
        List<Map<String, Object>> result = sqlEngine.executeQuery(
            "SELECT balance FROM accounts WHERE id = 1").get();
        
        // Transaction 2 rolls back
        tx2.rollback().get();

        // Verify final state
        tx1.commit().get();
        Transaction tx3 = sqlEngine.beginTransaction().get();
        List<Map<String, Object>> finalResult = sqlEngine.executeQuery(
            "SELECT balance FROM accounts WHERE id = 1").get();
        
        // Final balance should be different from what tx1 read
        assertNotEquals(
            result.get(0).get("balance"),
            finalResult.get(0).get("balance")
        );
        tx3.commit().get();
    }

 */

 private void setupTestData(TransactionManager transactionManager) {
    ExecutionContext executionContext = new ExecutionContext(transactionManager);
        // Create accounts table
        sqlEngine.executeUpdate("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                balance INTEGER
            )
            """, executionContext);

        // Insert test data
        sqlEngine.executeUpdate("""
            INSERT INTO accounts (id, user_id, balance) VALUES 
            (1, 11, 1000),
            (2, 12, 2000),
            (3, 13, 3000)
            """, executionContext);

        executionContext.commitTransaction();
}
} 