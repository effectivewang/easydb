package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.ParseTree;
import java.util.List;

/**
 * Interface for SQL statement parsers.
 * Each implementation handles a specific type of SQL statement.
 */
public interface SqlParser {
    /**
     * Parse a list of tokens into a parse tree.
     * 
     * @param tokens The list of tokens to parse
     * @return The root node of the parse tree
     * @throws ParseException if the tokens cannot be parsed into a valid SQL statement
     */
    ParseTree parse(List<Token> tokens);

    /**
     * Check if this parser can handle the given tokens.
     * 
     * @param tokens The list of tokens to check
     * @return true if this parser can handle the tokens, false otherwise
     */
    boolean canHandle(List<Token> tokens);
} 