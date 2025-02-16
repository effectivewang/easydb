package com.easydb.sql.planner;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.sql.planner.QueryTree;
import com.easydb.core.DataType;
import com.easydb.core.Column;
import com.easydb.sql.planner.QueryOperator;
import com.easydb.sql.planner.QueryPredicate;

class QueryTreeTest {

    private TableMetadata usersTable = new TableMetadata(
        "users", 
        Arrays.asList(new Column("id", DataType.INTEGER), new Column("name", DataType.STRING), new Column("age", DataType.INTEGER)), 
        new HashMap<String, IndexMetadata>(), 
        new ArrayList<>());
    private TableMetadata departmentsTable = new TableMetadata(
        "departments", 
        Arrays.asList(new Column("id", DataType.INTEGER), new Column("name", DataType.STRING), new Column("department", DataType.STRING), new Column("salary", DataType.INTEGER)), 
        new HashMap<String, IndexMetadata>(), 
        new ArrayList<>());
    private TableMetadata ordersTable = new TableMetadata(
        "orders", 
        Arrays.asList(new Column("id", DataType.INTEGER), new Column("user_id", DataType.INTEGER), new Column("total", DataType.INTEGER)), 
        new HashMap<String, IndexMetadata>(), 
        new ArrayList<>());
   
    @Test
    void testSimpleQueryTree() {
        QueryPredicate predicate = QueryPredicate.equals("age", 25);
        QueryTree tree = new QueryTree(
            QueryOperator.SEQUENTIAL_SCAN, 
            predicate, 
            Arrays.asList("id", "name", "age"), 
            Arrays.asList(new RangeTableEntry(
                "users", 
                null, 
                usersTable,
                1
            ))
        );

        assertEquals(QueryOperator.SEQUENTIAL_SCAN, tree.getOperator());
        assertEquals(predicate, (QueryPredicate)tree.getOperation());
        assertEquals(Arrays.asList("id", "name", "age"), tree.getOutputColumns());
        assertTrue(tree.getChildren().isEmpty());
    }

    @Test
    void testQueryTreeWithChildren() {
        // Create leaf nodes
        QueryTree scan1 = new QueryTree(
            QueryOperator.SEQUENTIAL_SCAN,
            null,
            Arrays.asList("id", "name", "age"),
            Arrays.asList(new RangeTableEntry(
                "users", 
                null, 
                usersTable,
                1
            ))
        );

        QueryTree scan2 = new QueryTree(
            QueryOperator.SEQUENTIAL_SCAN,
            null,
            Arrays.asList("id", "department", "salary"),
            Arrays.asList(new RangeTableEntry(
                "departments", 
                null, 
                departmentsTable,
                2
            ))
        );

        // Create join node
        QueryTree join = new QueryTree(
            QueryOperator.HASH_JOIN,
            QueryPredicate.equals("id", "id"),
            Arrays.asList("name", "department", "salary"),
            Arrays.asList(new RangeTableEntry(
                "departments", 
                null, 
                departmentsTable,
                2
            ))
        );

        join.addChild(scan1);
        join.addChild(scan2);

        assertEquals(2, join.getChildren().size());
        assertEquals(scan1, join.getChildren().get(0));
        assertEquals(scan2, join.getChildren().get(1));
    }

    @Test
    void testQueryTreeWithEstimates() {
        QueryTree tree = new QueryTree(
            QueryOperator.SEQUENTIAL_SCAN,
            null,
            Arrays.asList("*"),
            Arrays.asList(new RangeTableEntry(
                "users", 
                null, 
                usersTable, 
                1))
        );

        tree.setEstimatedCost(100.5);
        tree.setEstimatedRows(1000);

        assertEquals(100.5, tree.getEstimatedCost(), 0.001);
        assertEquals(1000, tree.getEstimatedRows());
    }

    @Test
    void testComplexQueryTree() {
        // Create table scans
        QueryTree usersScan = new QueryTree(
            QueryOperator.SEQUENTIAL_SCAN,
            QueryPredicate.greaterThan("age", 18),
            Arrays.asList("id", "name", "age"),
            Arrays.asList(new RangeTableEntry("users", null, 
                usersTable, 1))
        );
        usersScan.setEstimatedRows(1000);
        usersScan.setEstimatedCost(100.0);

        QueryTree ordersScan = new QueryTree(
            QueryOperator.INDEX_SCAN,
            QueryPredicate.equals("status", "pending"),
            Arrays.asList("id", "user_id", "total"),
            Arrays.asList(new RangeTableEntry(
                "orders", 
                null, 
                ordersTable, 
                2))
            );
        ordersScan.setEstimatedRows(500);
        ordersScan.setEstimatedCost(50.0);

        // Create join
        QueryTree join = new QueryTree(
            QueryOperator.HASH_JOIN,
            QueryPredicate.equals("id", "user_id"),
            Arrays.asList("name", "total"),
            Arrays.asList(new RangeTableEntry(
                "orders", 
                null, 
                ordersTable, 
                2))
        );
        join.setEstimatedRows(1500);
        join.setEstimatedCost(200.0);

        join.addChild(usersScan);
        join.addChild(ordersScan);

        // Create aggregation
        QueryTree aggregate = new QueryTree(
            QueryOperator.HASH_AGGREGATE,
            null,
            Arrays.asList("name", "total_orders"),
            Arrays.asList(new RangeTableEntry(
                "orders", 
                null, 
                ordersTable, 
                2))
        );
        aggregate.setEstimatedRows(100);
        aggregate.setEstimatedCost(300.0);

        aggregate.addChild(join);

        // Verify the tree structure
        assertEquals(1, aggregate.getChildren().size());
        assertEquals(2, aggregate.getChildren().get(0).getChildren().size());
        assertEquals(300.0, aggregate.getEstimatedCost(), 0.001);
        assertEquals(100, aggregate.getEstimatedRows());

        // Verify toString output contains all operators and estimates
        String treeString = aggregate.toString();
        assertTrue(treeString.contains("Hash Aggregate"));
        assertTrue(treeString.contains("Hash Join"));
        assertTrue(treeString.contains("Seq Scan"));
        assertTrue(treeString.contains("Index Scan"));
        assertTrue(treeString.contains("cost="));
        assertTrue(treeString.contains("rows="));
    }

    @Test
    void testToString() {
        QueryTree scan = new QueryTree(
            QueryOperator.SEQUENTIAL_SCAN,
            QueryPredicate.equals("age", 25),
            Arrays.asList("id", "name", "age"),
            Arrays.asList(new RangeTableEntry(
                "users", 
                null, 
                usersTable, 
                1))
        );
        scan.setEstimatedCost(100.0);
        scan.setEstimatedRows(1000);

        String output = scan.toString();
        System.out.println(output);
        assertTrue(output.contains("Seq Scan"));
        assertTrue(output.contains("age = 25"));
        assertTrue(output.contains("[id, name, age]"));
        assertTrue(output.contains("cost=100.00"));
        assertTrue(output.contains("rows=1000"));
    }
} 