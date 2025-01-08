package com.easydb.sql.command;

import com.easydb.storage.Storage;
import com.easydb.sql.result.ResultSet;
import com.easydb.core.Column;
import com.easydb.core.DataType;
import com.easydb.core.Transaction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a SQL SELECT command.
 */
public class SelectCommand implements SqlCommand {
    private final String tableName;
    private final List<String> columns;
    private final Map<String, Object> conditions;

    public SelectCommand(String tableName, List<String> columns, Map<String, Object> conditions) {
        this.tableName = tableName;
        this.columns = columns;
        this.conditions = conditions;
    }

    @Override
    public CompletableFuture<Object> execute(Storage storage) {
        return execute(storage, null);
    }

    @Override
    public SqlCommandType getType() {
        return SqlCommandType.SELECT;
    }

    @Override
    public CompletableFuture<Object> execute(Storage storage, Transaction txn) {
        throw new UnsupportedOperationException("Not implemented");
    }
} 