package com.easydb.sql.parser;

import com.easydb.sql.command.SqlCommand;
import com.easydb.storage.Storage;
import com.easydb.storage.transaction.TransactionManager;

public class SqlParserFactory {
    private final CreateTableParser createTableParser;
    private final CreateIndexParser createIndexParser;
    private final InsertParser insertParser;
    private final SelectParser selectParser;
    private final TransactionParser transactionParser;
    private final Storage storage;
    private final TransactionManager transactionManager;
    
    public SqlParserFactory(Storage storage, TransactionManager transactionManager) {
        this.storage = storage;
        this.transactionManager = transactionManager;
        this.createTableParser = new CreateTableParser();
        this.createIndexParser = new CreateIndexParser();
        this.insertParser = new InsertParser(storage);
        this.selectParser = new SelectParser();
        this.transactionParser = new TransactionParser(transactionManager, this);
    }

    public SqlCommand parse(String sql) {        
        if (sql.startsWith("CREATE TABLE")) {
            return createTableParser.parse(sql);
        } else if (sql.startsWith("CREATE INDEX")) {
            return createIndexParser.parse(sql);
        } else if (sql.startsWith("INSERT")) {
            return insertParser.parse(sql);
        } else if (sql.startsWith("SELECT")) {
            return selectParser.parse(sql);
        } else if (sql.startsWith("SET TRANSACTION")) {
            return transactionParser.parse(sql);
        }
        throw new IllegalArgumentException("Unsupported SQL command: " + sql);
    }

    // Getters for individual parsers if needed
    public CreateTableParser getCreateTableParser() {
        return createTableParser;
    }

    public CreateIndexParser getCreateIndexParser() {
        return createIndexParser;
    }

    public InsertParser getInsertParser() {
        return insertParser;
    }

    public SelectParser getSelectParser() {
        return selectParser;
    }

    public TransactionParser getTransactionParser() {
        return transactionParser;
    }

} 