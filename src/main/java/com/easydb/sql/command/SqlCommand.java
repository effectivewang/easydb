package com.easydb.sql.command;

import com.easydb.storage.Storage;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for SQL commands.
 */
public interface SqlCommand {
    CompletableFuture<Object> execute(Storage storage);
    SqlCommandType getType();
} 