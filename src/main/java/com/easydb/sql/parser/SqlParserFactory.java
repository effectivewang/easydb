package com.easydb.sql.parser;

import com.easydb.sql.parser.token.Token;
import com.easydb.sql.parser.token.TokenType;
import com.easydb.sql.parser.ParseTree;

import java.util.List;
import java.util.ArrayList;

/**
 * Factory class for creating SQL statement parsers.
 * Determines the appropriate parser based on the SQL statement type.
 */
public class SqlParserFactory {
    private final List<Parser> parsers;

    public SqlParserFactory() {
        this.parsers = new ArrayList<>();
    }

    /**
     * Parse a SQL statement into a parse tree.
     * 
     * @param sql The SQL statement to parse
     * @return The root node of the parse tree
     * @throws ParseException if the SQL statement cannot be parsed
     */
    public ParseTree parse(String sql) {
        Lexer lexer = new Lexer(sql);
        List<com.easydb.sql.parser.token.Token> tokens = lexer.tokenize();
        
        switch (tokens.get(0).type()) {
            case TokenType.CREATE:
                if(tokens.get(1).type() == TokenType.INDEX) {
                    return new CreateIndexParser(tokens).parse();
                } else if(tokens.get(1).type() == TokenType.TABLE) {
                    return new CreateTableParser(tokens).parse();
                }
            case TokenType.INSERT:
                return new InsertParser(tokens).parse();
            case TokenType.SELECT:
                return new SelectParser(tokens).parse();
            case TokenType.UPDATE:
                return new UpdateParser(tokens).parse();
            case TokenType.DELETE:
                return new DeleteParser(tokens).parse();
            case TokenType.SET:
                return new SetTransactionParser(tokens).parse();
            default:
                throw new ParseException(tokens.get(0), "Unsupported SQL statement type");
        }
    }
} 