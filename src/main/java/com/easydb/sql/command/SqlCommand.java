package com.easydb.sql.command;

import com.easydb.storage.Storage;
import com.easydb.core.Transaction;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for SQL commands.
 */
public interface SqlCommand {
    CompletableFuture<Object> execute(Storage storage);
    CompletableFuture<Object> execute(Storage storage, Transaction txn);
    SqlCommandType getType();
} 