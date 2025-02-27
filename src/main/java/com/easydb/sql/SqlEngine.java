package com.easydb.sql;

import com.easydb.sql.result.ResultSet;
import java.util.concurrent.CompletableFuture;
import com.easydb.sql.parser.ParseTree;
import com.easydb.storage.transaction.Transaction;
import com.easydb.sql.executor.ExecutionContext;
/**
 * Interface for executing SQL commands.
 */
public interface SqlEngine {
    /**
     * Execute a SQL query that returns a result set.
     */
    ResultSet executeQuery(String sql, ExecutionContext executionContext);

    /**
     * Execute a SQL update that returns the number of affected rows.
     */
    Integer executeUpdate(String sql, ExecutionContext executionContext);
}

/**
 * Interface for prepared statements.
 */
interface PreparedStatement {
    /**
     * Execute the prepared statement.
     *
     * @param parameters The parameters for the statement
     * @return A future that completes with the result set
     */
    ResultSet execute(Object... parameters);
}