package com.easydb.sql.parser;

import com.easydb.sql.command.SqlCommand;

/**
 * Interface for SQL statement parsers.
 */
public interface SqlParser {
    /**
     * Parses a SQL statement into a command object.
     *
     * @param sql The SQL statement to parse
     * @return The parsed command
     * @throws IllegalArgumentException if the SQL statement is invalid
     */
    SqlCommand parse(String sql);
} 