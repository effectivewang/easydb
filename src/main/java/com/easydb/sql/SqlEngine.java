package com.easydb.sql;

import com.easydb.sql.result.ResultSet;
import java.util.concurrent.CompletableFuture;
import com.easydb.sql.parser.ParseTree;
import com.easydb.storage.transaction.Transaction;
/**
 * Interface for executing SQL commands.
 */
public interface SqlEngine {
    /**
     * Execute a SQL query that returns a result set.
     */
    ResultSet executeQuery(String sql);

    /**
     * Execute a SQL update that returns the number of affected rows.
     */
    Integer executeUpdate(String sql);

    /**
     * Execute a SQL command.
     */
    Object execute(ParseTree parseTree);

    /**
     * Prepare a SQL statement for execution.
     */
    PreparedStatement prepareStatement(String sql);

    /**
     * Begin a new transaction.
     */
    Transaction beginTransaction();
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