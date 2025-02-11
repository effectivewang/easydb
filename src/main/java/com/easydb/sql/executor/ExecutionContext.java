package com.easydb.sql.executor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.easydb.storage.transaction.Transaction;
import com.easydb.storage.transaction.IsolationLevel;

public class ExecutionContext {
    private final Transaction currentTransaction;
    private final Map<String, List<Transaction>> transactions;

    public ExecutionContext() {
        this.currentTransaction = null;
        this.transactions = new ConcurrentHashMap<>();
    }

    public Transaction getTransaction() {
        return currentTransaction;
    }

    public Transaction startNewTransaction(String tableName) {
        Transaction transaction = new Transaction(System.currentTimeMillis(), IsolationLevel.READ_COMMITTED);
        transactions.computeIfAbsent(tableName, k -> new ArrayList<>()).add(transaction);
        return transaction;
    }
}
