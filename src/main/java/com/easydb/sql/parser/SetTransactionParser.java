package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.storage.transaction.IsolationLevel;
import java.util.List;

public class SetTransactionParser extends Parser {
    
    public SetTransactionParser(List<Token> tokens) {
        super(tokens);
    }

    @Override
    public ParseTree parse() {
        // SET TRANSACTION
        consume(TokenType.SET, "Expected 'SET'");
        consume(TokenType.TRANSACTION, "Expected 'TRANSACTION'");
        
        // ISOLATION LEVEL
        consume(TokenType.ISOLATION, "Expected 'ISOLATION'");
        consume(TokenType.LEVEL, "Expected 'LEVEL'");

        IsolationLevel level = parseIsolationLevel();
        
        // Optional semicolon
        match(TokenType.SEMICOLON);

        return new ParseTree(ParseTreeType.SET_TRANSACTION_STATEMENT, level.name());
    }

    private IsolationLevel parseIsolationLevel() {
        if (match(TokenType.READ)) {
            if (match(TokenType.COMMITTED)) {
                return IsolationLevel.READ_COMMITTED;
            }
            if (match(TokenType.UNCOMMITTED)) {
                return IsolationLevel.READ_UNCOMMITTED;
            }
            throw error(peek(), "Invalid READ isolation level");
        }
        
        if (match(TokenType.REPEATABLE)) {
            consume(TokenType.READ, "Expected 'READ' after 'REPEATABLE'");
            return IsolationLevel.REPEATABLE_READ;
        }
        
        if (match(TokenType.SERIALIZABLE)) {
            return IsolationLevel.SERIALIZABLE;
        }

        throw error(peek(), "Expected isolation level");
    }
} 