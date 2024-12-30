package com.easydb.sql.command;

import com.easydb.storage.Storage;
import com.easydb.sql.result.ResultSet;
import com.easydb.core.Column;
import com.easydb.core.DataType;
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
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public SqlCommandType getType() {
        return SqlCommandType.SELECT;
    }
} 