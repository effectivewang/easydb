package com.easydb.sql.parser;

import com.easydb.sql.command.SqlCommand;

public class SqlParserFactory {
    private final CreateTableParser createTableParser;
    private final CreateIndexParser createIndexParser;
    private final InsertParser insertParser;
    private final SelectParser selectParser;

    public SqlParserFactory() {
        this.createTableParser = new CreateTableParser();
        this.createIndexParser = new CreateIndexParser();
        this.insertParser = new InsertParser();
        this.selectParser = new SelectParser();
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

} 