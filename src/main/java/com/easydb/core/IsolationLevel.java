package com.easydb.core;

// Isolation Level Definition
public enum IsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE,
    SNAPSHOT
}
