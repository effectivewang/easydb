package com.easydb.sql;

import com.easydb.sql.result.ResultSet;
import com.easydb.sql.command.SqlCommand;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for executing SQL commands.
 */
public interface SqlEngine {
    /**
     * Execute a SQL query that returns a result set.
     */
    CompletableFuture<ResultSet> executeQuery(String sql);

    /**
     * Execute a SQL update that returns the number of affected rows.
     */
    CompletableFuture<Integer> executeUpdate(String sql);

    /**
     * Execute a SQL command.
     */
    CompletableFuture<Object> execute(SqlCommand command);

    /**
     * Prepare a SQL statement for execution.
     */
    CompletableFuture<PreparedStatement> prepareStatement(String sql);

    /**
     * Begin a new transaction.
     */
    CompletableFuture<Transaction> beginTransaction();
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
    CompletableFuture<ResultSet> execute(Object... parameters);
}

/**
 * Interface for transactions.
 */
interface Transaction {
    /**
     * Commit the transaction.
     *
     * @return A future that completes when the commit is done
     */
    CompletableFuture<Void> commit();

    /**
     * Rollback the transaction.
     *
     * @return A future that completes when the rollback is done
     */
    CompletableFuture<Void> rollback();
} 