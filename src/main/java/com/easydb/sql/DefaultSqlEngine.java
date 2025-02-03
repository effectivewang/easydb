package com.easydb.sql;

import com.easydb.sql.result.ResultSet;
import com.easydb.storage.InMemoryStorage;
import com.easydb.storage.Tuple;
import com.easydb.storage.transaction.Transaction;
import com.easydb.storage.transaction.IsolationLevel;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.SqlParserFactory;
import com.easydb.sql.planner.QueryTree;
import com.easydb.sql.executor.QueryExecutor;
import com.easydb.sql.executor.ExecutionContext;
import com.easydb.sql.planner.QueryTreeGenerator;

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
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String sql) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Transaction beginTransaction() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet executeQuery(String sql) {
        ExecutionContext executionContext = new ExecutionContext();
        ParseTree parseTree = parserFactory.parse(sql);
        QueryTreeGenerator queryTreeGenerator = new QueryTreeGenerator(storage);
        QueryTree queryTree = queryTreeGenerator.generate(parseTree);
        QueryExecutor queryExecutor = new QueryExecutor(storage, executionContext);
        List<Tuple> qTuples = queryExecutor.execute(queryTree, executionContext);
        return new ResultSet.Builder().build(qTuples, storage);
    }
} 