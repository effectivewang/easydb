package com.easydb.core;

/**
 * Represents the possible states of a database transaction.
 */
public enum TransactionStatus {
    /**
     * Transaction is currently active and can accept operations
     */
    ACTIVE,

    /**
     * Transaction has been prepared for commit in 2PC protocol
     */
    PREPARED,

    /**
     * Transaction has been successfully committed
     */
    COMMITTED,

    /**
     * Transaction has been aborted and rolled back
     */
    ABORTED
} 