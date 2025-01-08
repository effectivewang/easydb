package com.easydb.sql.parser;

import com.easydb.core.IsolationLevel;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.lang.IllegalArgumentException;
import com.easydb.sql.command.SqlCommand;
import com.easydb.storage.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.List;
import com.easydb.sql.command.TransactionCommand;

public class TransactionParser implements SqlParser  {
    private static final Pattern SET_ISOLATION_PATTERN = Pattern.compile(
        "SET\\s+TRANSACTION\\s+ISOLATION\\s+LEVEL\\s+(READ\\s+UNCOMMITTED|READ\\s+COMMITTED|REPEATABLE\\s+READ|SERIALIZABLE|SNAPSHOT)",
        Pattern.CASE_INSENSITIVE
    );

    private final TransactionManager transactionManager;
    private final SqlParserFactory sqlParserFactory;

    public TransactionParser(TransactionManager transactionManager, SqlParserFactory sqlParserFactory) {
        this.transactionManager = transactionManager;
        this.sqlParserFactory = sqlParserFactory;
    }

    @Override
    public SqlCommand parse(String sql) {
        Matcher matcher = SET_ISOLATION_PATTERN.matcher(sql.trim());
        
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid SET TRANSACTION ISOLATION LEVEL syntax");
        }

        String level = matcher.group(1).toUpperCase().replace(" ", "_");
        
        try {
            IsolationLevel isolationLevel = IsolationLevel.valueOf(level);
            return new TransactionCommand(transactionManager, isolationLevel, new ArrayList<>());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported isolation level: " + level);
        }
    }

}