package com.easydb.sql.command;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.storage.TupleId;
import com.easydb.storage.metadata.TableMetadata;
/**
 * Represents a SQL INSERT command.
 */
public class InsertCommand implements SqlCommand {

    private final String tableName;
    private final List<String> columns;
    private final List<List<Object>> values;

    public InsertCommand(String tableName, List<String> columns, List<List<Object>> values) {
        this.tableName = tableName;
        this.columns = columns;
        this.values = values;
    }

    @Override
    public SqlCommandType getType() {
        return SqlCommandType.INSERT;
    }

    @Override
    public CompletableFuture<Object> execute(Storage storage) {
        
        storage.getTableMetadata(tableName).join();
        return CompletableFuture.supplyAsync(() -> {
                for (List<Object> value : values) {
                    Tuple tuple = new Tuple(TupleId.create(tableName), value);
                    storage.insertTuple(tuple);
                }
                return values.size();
            });
    }
} 