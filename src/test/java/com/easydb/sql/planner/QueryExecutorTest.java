package com.easydb.sql.planner;

import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.storage.transaction.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class QueryExecutorTest {
    @Mock
    private Storage storage;
    
    @Mock
    private Transaction transaction;
    
    private QueryExecutor executor;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        executor = new QueryExecutor(storage, transaction);
    }
    
    @Test
    void testSequentialScan() {
        // Setup
        QueryTree tree = new QueryTree(QueryOperator.SEQUENTIAL_SCAN);
        tree.addOutputColumn("users.id");
        
        List<Tuple> expectedTuples = Arrays.asList(
            new Tuple(Arrays.asList(1)),
            new Tuple(Arrays.asList(2))
        );
        
        when(storage.findTuplesInTransaction(eq("users"), any())).thenReturn(expectedTuples);
        
        // Execute
        List<Tuple> result = executor.execute(tree);
        
        // Verify
        assertEquals(expectedTuples, result);
    }
    
    @Test
    void testParallelHashJoin() {
        // Setup
        QueryTree leftScan = new QueryTree(QueryOperator.SEQUENTIAL_SCAN);
        leftScan.addOutputColumn("users.id");
        leftScan.addOutputColumn("users.name");
        
        QueryTree rightScan = new QueryTree(QueryOperator.SEQUENTIAL_SCAN);
        rightScan.addOutputColumn("orders.user_id");
        rightScan.addOutputColumn("orders.amount");
        
        QueryTree joinTree = new QueryTree(QueryOperator.HASH_JOIN);
        joinTree.addChild(leftScan);
        joinTree.addChild(rightScan);
        joinTree.setPredicate(QueryPredicate.equals("users.id", "orders.user_id"));
        
        List<Tuple> leftTuples = Arrays.asList(
            new Tuple(Arrays.asList(1, "Alice")),
            new Tuple(Arrays.asList(2, "Bob"))
        );
        
        List<Tuple> rightTuples = Arrays.asList(
            new Tuple(Arrays.asList(1, 100)),
            new Tuple(Arrays.asList(1, 200))
        );
        
        when(storage.findTuplesInTransaction(eq("users"), any())).thenReturn(leftTuples);
        when(storage.findTuplesInTransaction(eq("orders"), any())).thenReturn(rightTuples);
        
        // Execute
        List<Tuple> result = executor.execute(joinTree);
        
        // Verify
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(t -> t.getValues().size() == 4));
        assertTrue(result.stream().allMatch(t -> t.getValues().get(0).equals(1)));
    }
    
    @Test
    void testConcurrentExecution() throws InterruptedException {
        // Setup
        int numThreads = 4;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numThreads);
        ExecutorService testExecutor = Executors.newFixedThreadPool(numThreads);
        
        QueryTree tree = new QueryTree(QueryOperator.SEQUENTIAL_SCAN);
        tree.addOutputColumn("users.id");
        
        List<Tuple> expectedTuples = Arrays.asList(
            new Tuple(Arrays.asList(1)),
            new Tuple(Arrays.asList(2))
        );
        
        when(storage.findTuplesInTransaction(eq("users"), any())).thenReturn(expectedTuples);
        
        // Execute concurrent queries
        for (int i = 0; i < numThreads; i++) {
            testExecutor.submit(() -> {
                try {
                    startLatch.await();
                    List<Tuple> result = executor.execute(tree);
                    assertEquals(expectedTuples, result);
                } catch (InterruptedException e) {
                    fail("Thread interrupted");
                } finally {
                    completionLatch.countDown();
                }
            });
        }
        
        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for completion
        assertTrue(completionLatch.await(5, TimeUnit.SECONDS));
        testExecutor.shutdown();
    }
    
    @Test
    void testTransactionIsolation() {
        // Setup
        QueryTree tree = new QueryTree(QueryOperator.SEQUENTIAL_SCAN);
        tree.addOutputColumn("users.id");
        
        List<Tuple> expectedTuples = Arrays.asList(
            new Tuple(Arrays.asList(1)),
            new Tuple(Arrays.asList(2))
        );
        
        when(storage.findTuplesInTransaction(eq("users"), any())).thenReturn(expectedTuples);
        
        // Execute
        List<Tuple> result = executor.execute(tree);
        
        // Verify that transaction was used
        assertEquals(expectedTuples, result);
    }
    
    @Test
    void testComplexQueryPlan() {
        // Setup a complex query plan with multiple operations
        QueryTree scan = new QueryTree(QueryOperator.SEQUENTIAL_SCAN);
        scan.addOutputColumn("users.id");
        scan.addOutputColumn("users.age");
        
        QueryTree filter = new QueryTree(QueryOperator.FILTER);
        filter.addChild(scan);
        filter.setPredicate(QueryPredicate.greaterThan("users.age", 18));
        
        QueryTree sort = new QueryTree(QueryOperator.SORT);
        sort.addChild(filter);
        sort.addOutputColumn("users.age");
        
        List<Tuple> scanTuples = Arrays.asList(
            new Tuple(Arrays.asList(1, 25)),
            new Tuple(Arrays.asList(2, 17)),
            new Tuple(Arrays.asList(3, 30))
        );
        
        when(storage.findTuplesInTransaction(eq("users"), any())).thenReturn(scanTuples);
        
        // Execute
        List<Tuple> result = executor.execute(sort);
        
        // Verify
        assertEquals(2, result.size());
        assertTrue((Integer)result.get(0).getValues().get(1) < (Integer)result.get(1).getValues().get(1));
    }
} 