package com.easydb.sql.command;

import com.easydb.core.IsolationLevel;
import com.easydb.storage.Storage;
import com.easydb.storage.transaction.TransactionManager;
import java.util.concurrent.CompletableFuture;
import java.util.List;
import com.easydb.core.Transaction;
public class TransactionCommand implements SqlCommand {
    private final TransactionManager transactionManager;
    private final IsolationLevel isolationLevel;
    private final List<SqlCommand> commands;

    public TransactionCommand(TransactionManager transactionManager, IsolationLevel isolationLevel, List<SqlCommand> commands) {
        this.transactionManager = transactionManager;
        this.isolationLevel = isolationLevel;
        this.commands = commands;
    }

    @Override
    public SqlCommandType getType() {
        return SqlCommandType.TRANSACTION;
    }

    @Override
    public CompletableFuture<Object> execute(Storage storage, Transaction txn) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Object> execute(Storage storage) {
        Transaction txn = transactionManager.beginTransaction(isolationLevel);
        
        commands.stream().map(cmd -> cmd.execute(storage, txn));
        return CompletableFuture.completedFuture(commands.size());
    }
}
