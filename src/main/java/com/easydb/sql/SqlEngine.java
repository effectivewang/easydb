package com.easydb.sql;

import com.easydb.sql.result.ResultSet;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for SQL operations in EasyDB.
 */
public interface SqlEngine {
    /**
     * Execute a SQL query.
     *
     * @param sql The SQL query to execute
     * @return A future that completes with the result set
     */
    CompletableFuture<ResultSet> executeQuery(String sql);

    /**
     * Execute a SQL update statement.
     *
     * @param sql The SQL update statement to execute
     * @return A future that completes with the number of rows affected
     */
    CompletableFuture<Integer> executeUpdate(String sql);

    /**
     * Prepare a SQL statement.
     *
     * @param sql The SQL statement to prepare
     * @return A future that completes with the prepared statement
     */
    CompletableFuture<PreparedStatement> prepareStatement(String sql);

    /**
     * Begin a transaction.
     *
     * @return A future that completes with the transaction
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