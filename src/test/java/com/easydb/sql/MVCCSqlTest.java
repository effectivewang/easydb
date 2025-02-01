package com.easydb.sql;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class MVCCSqlTest {
    private SqlEngine sqlEngine;

    @BeforeEach
    void setUp() {
        sqlEngine = SqlEngineFactory.create();
        setupTestData();
    }

    @Test
    void testReadCommitted() throws ExecutionException, InterruptedException {
        // Start transaction 1 with READ COMMITTED isolation
        Transaction tx1 = sqlEngine.beginTransaction(IsolationLevel.READ_COMMITTED);
        // Start transaction 2
        Transaction tx2 = sqlEngine.beginTransaction(IsolationLevel.READ_COMMITTED);

        // Transaction 1 reads data
        List<ResultSet> result1 = tx1.executeQuery("SELECT balance FROM accounts WHERE id = 1");
        int initialBalance = (Integer) result1.get(0).get("balance");

        // Transaction 2 updates and commits
        tx2.executeUpdate("UPDATE accounts SET balance = balance + 100 WHERE id = 1").get();
        List<ResultSet> result2 = tx2.executeQuery("SELECT balance FROM accounts WHERE id = 1");
        int uncommittedBalance = (Integer) result2.get(0).get("balance");
        assertEquals(initialBalance + 100, uncommittedBalance);

        result1 = tx1.executeQuery("SELECT balance FROM accounts WHERE id = 1");
        assertEquals(initialBalance, (Integer) result1.get(0).get("balance"));
        tx2.commit().get();

        // Transaction 1 reads again - should see new data (no repeatable read)
        result1 = tx1.executeQuery("SELECT balance FROM accounts WHERE id = 1").get();
        int newBalance = (Integer) result1.get(0).get("balance");

        assertEquals(initialBalance + 100, newBalance);
        tx1.commit().get();
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
    private void setupTestData() {
        try {
            // Create accounts table
            sqlEngine.executeUpdate("""
                CREATE TABLE accounts (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    balance INTEGER
                )
                """).get();

            // Insert test data
            sqlEngine.executeUpdate("""
                INSERT INTO accounts (id, user_id, balance) VALUES 
                (1, 1, 1000),
                (2, 2, 2000),
                (3, 3, 3000)
                """).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Failed to setup test data", e);
        }
    }
} 