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

import java.util.*;
import java.util.stream.Collectors;

public class DefaultSqlEngine implements SqlEngine {
    private final InMemoryStorage storage;
    private final SqlParserFactory parserFactory;

    public DefaultSqlEngine(InMemoryStorage storage) {
        this.storage = storage;
        this.parserFactory = new SqlParserFactory();
    }

    @Override
    public Integer executeUpdate(String sql) {
        ParseTree parseTree = parserFactory.parse(sql);

        // Handle DDL separately
        if (isDDLStatement(parseTree)) {
            return executeDDL(parseTree);
        }

        QueryTreeGenerator queryTreeGenerator = new QueryTreeGenerator(storage);
        ExecutionContext executionContext = new ExecutionContext();
        QueryExecutor queryExecutor = new QueryExecutor(storage, executionContext);
        QueryTree queryTree = queryTreeGenerator.generate(parseTree);

        System.out.println(queryTree.toString());
        List<Tuple> tuples = queryExecutor.execute(queryTree, executionContext);
        return tuples.size();
    }

    @Override
    public ResultSet executeQuery(String sql) {
        ExecutionContext executionContext = new ExecutionContext();
        QueryExecutor queryExecutor = new QueryExecutor(storage, executionContext);
        QueryTree queryTree = generaQueryTree(sql);

        List<Tuple> qTuples = queryExecutor.execute(queryTree, executionContext);
        return new ResultSet.Builder().build(qTuples, storage);
    }

    private QueryTree generaQueryTree(String sql) {
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
} 