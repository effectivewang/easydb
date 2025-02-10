package com.easydb.sql.parser;

/**
 * Defines the types of nodes in the SQL parse tree.
 */
public enum ParseTreeType {
    // Statements
    SELECT_STATEMENT,
    INSERT_STATEMENT,
    UPDATE_STATEMENT,
    DELETE_STATEMENT,
    CREATE_TABLE_STATEMENT,
    DROP_TABLE_STATEMENT,
    CREATE_INDEX_STATEMENT,
    DROP_INDEX_STATEMENT,

    // Clauses
    SELECT_LIST,
    FROM_CLAUSE,
    WHERE_CLAUSE,
    GROUP_BY_CLAUSE,
    HAVING_CLAUSE,
    ORDER_BY_CLAUSE,
    VALUES_CLAUSE,
    SET_CLAUSE,

    // Expressions
    COLUMN_REF,
    TABLE_REF,
    INDEX_REF,
    COLUMN_LIST,
    LITERAL,
    BINARY_EXPR,
    UNARY_EXPR,
    FUNCTION_CALL,
    SUBQUERY,
    CASE_EXPR,
    BETWEEN_EXPR,
    IN_EXPR,
    EXISTS_EXPR,
    LIKE_EXPR,
    NULL_EXPR,
    CONDITION,

    // Operators
    AND_OPERATOR,
    OR_OPERATOR,
    NOT_OPERATOR,
    EQUALS_OPERATOR,
    NOT_EQUALS_OPERATOR,
    LESS_THAN_OPERATOR,
    LESS_THAN_EQUALS_OPERATOR,
    GREATER_THAN_OPERATOR,
    GREATER_THAN_EQUALS_OPERATOR,
    PLUS_OPERATOR,
    MINUS_OPERATOR,
    MULTIPLY_OPERATOR,
    DIVIDE_OPERATOR,

    // Data types
    INTEGER_TYPE,
    STRING_TYPE,
    BOOLEAN_TYPE,
    DOUBLE_TYPE,
    NULL_TYPE,
    
    // Constraints
    CONSTRAINT_REF,
    TABLE_CONSTRAINTS,
    PRIMARY_KEY_CONSTRAINT,
    FOREIGN_KEY_CONSTRAINT,
    NOT_NULL_CONSTRAINT,
    CHECK_CONSTRAINT,
    UNIQUE_CONSTRAINT,

    // Other
    IDENTIFIER,
    ALIAS,
    PARAMETER,
    LIST,
    ASTERISK,
    ERROR,
    ASSIGNMENT
} 