package com.easydb.sql.planner;

/**
 * Base interface for all operations in query tree.
 * Similar to PostgreSQL's Node structure.
 */
public interface Operation {
    QueryOperator getOperator();
    
} 