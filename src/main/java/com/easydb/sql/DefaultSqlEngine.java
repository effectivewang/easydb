package com.easydb.sql;

import com.easydb.sql.result.ResultSet;
import com.easydb.storage.InMemoryStorage;
import com.easydb.storage.Tuple;
import com.easydb.storage.transaction.Transaction;
import com.easydb.storage.transaction.IsolationLevel;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import com.easydb.sql.parser.SqlParserFactory;
import com.easydb.sql.planner.QueryTree;
import com.easydb.sql.executor.QueryExecutor;
import com.easydb.sql.executor.ExecutionContext;
import com.easydb.sql.planner.QueryTreeGenerator;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.sql.ddl.*;
import com.easydb.storage.transaction.TransactionManager;
import com.easydb.storage.transaction.TransactionStatus;
import java.util.*;
import java.util.stream.Collectors;

public class DefaultSqlEngine implements SqlEngine {
    private final InMemoryStorage storage;
    private final SqlParserFactory parserFactory;
    private final TransactionManager transactionManager;

    public DefaultSqlEngine(InMemoryStorage storage) {
        this.transactionManager = new TransactionManager();
        this.storage = storage;
        this.parserFactory = new SqlParserFactory();
    }

    @Override
    public Integer executeUpdate(String sql, ExecutionContext executionContext) {
        ParseTree parseTree = parserFactory.parse(sql);

        if (isDDLStatement(parseTree)) {
            return executeDDL(parseTree);
        }

        if (parseTree.getType() == ParseTreeType.SET_TRANSACTION_STATEMENT) {
            handleSetTransaction(parseTree, executionContext);
            return 0;
        } else {
            executionContext.beginTransaction();
        }

        try {
            QueryTree queryTree = generateQueryTree(sql);
            QueryExecutor queryExecutor = new QueryExecutor(storage, executionContext);
            List<Tuple> results = queryExecutor.execute(queryTree);
            return results.size();
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public ResultSet executeQuery(String sql, ExecutionContext executionContext) {
        ParseTree parseTree = parserFactory.parse(sql);

        // Begin transaction for query
        if (parseTree.getType() == ParseTreeType.SET_TRANSACTION_STATEMENT) {
            handleSetTransaction(parseTree, executionContext);
            return ResultSet.empty();
        } else {
            executionContext.beginTransaction();
        }

        try {
            QueryTree queryTree = generateQueryTree(sql);
            QueryExecutor queryExecutor = new QueryExecutor(storage, executionContext);
            List<Tuple> results = queryExecutor.execute(queryTree);
            return new ResultSet.Builder().build(results);
        } catch (Exception e) {
            throw e;
        }
    }

    private QueryTree generateQueryTree(String sql) {
        ParseTree parseTree = parserFactory.parse(sql);
        QueryTreeGenerator queryTreeGenerator = new QueryTreeGenerator(storage);
        QueryTree queryTree = queryTreeGenerator.generate(parseTree);
        return queryTree;
    }

    private boolean isDDLStatement(ParseTree parseTree) {
        return parseTree.getType() ==  ParseTreeType.CREATE_TABLE_STATEMENT ||
               parseTree.getType() == ParseTreeType.CREATE_INDEX_STATEMENT;
    }

    private Integer executeDDL(ParseTree parseTree) {
        switch (parseTree.getType()) {
            case CREATE_TABLE_STATEMENT:
                return executeCreateTable(parseTree);
            case CREATE_INDEX_STATEMENT:
                return executeCreateIndex(parseTree);
            default:
                throw new IllegalStateException("Unknown DDL statement");
        }
    }

    public void commitTransaction(ExecutionContext executionContext) {
        executionContext.commitTransaction();
    }

    public void rollbackTransaction(ExecutionContext executionContext) {
        executionContext.rollbackTransaction();
    }

    private Integer executeCreateTable(ParseTree parseTree) {
        // Direct execution without query plan
        TableMetadata metadata = TableMetadataBuilder.fromParseTree(parseTree);
        storage.createTable(metadata);
        return 0; // Convention for DDL success
    }

    private Integer executeCreateIndex(ParseTree parseTree) {
        // Direct execution without query plan
        IndexMetadata metadata = IndexMetadataBuilder.fromParseTree(parseTree);
        storage.createIndex(metadata);
        return 0; // Convention for DDL success
    }

    private void handleSetTransaction(ParseTree parseTree, ExecutionContext executionContext) {
        IsolationLevel level = IsolationLevel.valueOf(parseTree.getValue());
        executionContext.setIsolationLevel(level);
    }
} 