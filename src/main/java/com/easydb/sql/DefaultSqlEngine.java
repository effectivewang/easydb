package com.easydb.sql;

import com.easydb.sql.result.ResultSet;
import com.easydb.storage.InMemoryStorage;
import com.easydb.storage.transaction.Transaction;
import com.easydb.storage.transaction.IsolationLevel;
import com.easydb.sql.parser.ParseTree;

import java.util.*;
import java.util.stream.Collectors;

public class DefaultSqlEngine implements SqlEngine {
    private final InMemoryStorage storage;

    public DefaultSqlEngine(InMemoryStorage storage) {
        this.storage = storage;
    }

    @Override
    public Object execute(ParseTree parseTree) {
        throw new UnsupportedOperationException("Not implemented");
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
        throw new UnsupportedOperationException("Not implemented");
    }
} 