package com.easydb.sql.command;

import com.easydb.core.Column;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.Storage;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CreateTableCommand implements SqlCommand {
    private final String tableName;
    private final List<Column> columns;

    public CreateTableCommand(String tableName, List<Column> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public CompletableFuture<Object> execute(Storage storage) {
        TableMetadata metadata = new TableMetadata(tableName, columns);
        return storage.createTable(metadata)
            .thenApply(v -> 1); // Return 1 to indicate one table was created
    }

    @Override
    public SqlCommandType getType() {
        return SqlCommandType.CREATE;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }
} 