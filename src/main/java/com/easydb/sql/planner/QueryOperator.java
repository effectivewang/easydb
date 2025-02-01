package com.easydb.sql.planner;

/**
 * Defines the types of operations that can appear in a query execution plan.
 */
public enum QueryOperator {
    // Scan operators
    SEQUENTIAL_SCAN("Seq Scan"),
    INDEX_SCAN("Index Scan"),
    INDEX_ONLY_SCAN("Index Only Scan"),

    // Join operators
    NESTED_LOOP_JOIN("Nested Loop"),
    HASH_JOIN("Hash Join"),
    MERGE_JOIN("Merge Join"),

    // Other operators
    FILTER("Filter"),
    PROJECT("Project"),
    SORT("Sort"),
    HASH_AGGREGATE("Hash Aggregate"),
    GROUP_AGGREGATE("Group Aggregate"),
    LIMIT("Limit"),
    DISTINCT("Distinct"),
    MATERIALIZE("Materialize"),
    APPEND("Append"),
    UNION("Union"),
    UNION_ALL("Union All"),
    INTERSECT("Intersect"),
    EXCEPT("Except");

    private final String displayName;

    QueryOperator(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }
} 