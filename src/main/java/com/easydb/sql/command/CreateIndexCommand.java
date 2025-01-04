package com.easydb.sql.command;

import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.storage.metadata.IndexType;
import com.easydb.storage.Storage;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CreateIndexCommand implements SqlCommand {
    private final String indexName;
    private final String tableName;
    private final List<String> columnNames;
    private final boolean isUnique;
    private final IndexType indexType;

    public CreateIndexCommand(
        String indexName,
        String tableName,
        List<String> columnNames,
        boolean isUnique,
        IndexType indexType
    ) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.isUnique = isUnique;
        this.indexType = indexType;
    }

    @Override
    public CompletableFuture<Object> execute(Storage storage) {
        IndexMetadata metadata = new IndexMetadata(
            indexName,
            tableName,
            columnNames,
            isUnique,
            indexType
        );
        
        return storage.createIndex(metadata)
            .thenApply(v -> 1); // Return 1 to indicate one index was created
    }

    @Override
    public SqlCommandType getType() {
        return SqlCommandType.CREATE;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public IndexType getIndexType() {
        return indexType;
    }
} 