package com.easydb;

import com.easydb.storage.InMemoryStorage;
import com.easydb.sql.DefaultSqlEngine;
import com.easydb.sql.SqlEngine;
import com.easydb.sql.command.SqlCommand;
import com.easydb.sql.parser.SqlParserFactory;
import com.easydb.sql.result.ResultSet;
import java.util.Scanner;

public class Main {
    private static void createTables(SqlEngine sqlEngine, SqlParserFactory parserFactory) {
        // Create users table with primary key and index
        String createTable = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name STRING NOT NULL,
                age INTEGER
            )""";
        
        String createIndex = "CREATE INDEX idx_id ON users (id)";
        
        try {
            SqlCommand tableCommand = parserFactory.parse(createTable);
            sqlEngine.execute(tableCommand).join();
            
            SqlCommand indexCommand = parserFactory.parse(createIndex);
            sqlEngine.execute(indexCommand).join();
            
            System.out.println("Database initialized successfully");
        } catch (Exception e) {
            System.err.println("Failed to initialize database: " + e.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        System.out.println("EasyDB - An educational database");
        
        // Initialize components
        InMemoryStorage storage = new InMemoryStorage();
        SqlEngine sqlEngine = new DefaultSqlEngine(storage);
        SqlParserFactory parserFactory = new SqlParserFactory(storage);
        Scanner scanner = new Scanner(System.in);

        // Initialize database schema
        createTables(sqlEngine, parserFactory);

        // REPL (Read-Eval-Print Loop)
        while (true) {
            System.out.print("sql> ");
            String sql = scanner.nextLine().trim();

            if (sql.equalsIgnoreCase("exit") || sql.equalsIgnoreCase("quit")) {
                break;
            }

            try {
                if (sql.toUpperCase().startsWith("SELECT")) {
                    ResultSet result = sqlEngine.executeQuery(sql).join();
                    printResultSet(result);
                } else {
                    SqlCommand command = parserFactory.parse(sql);
                    Object result = sqlEngine.execute(command).join();
                    System.out.println("Affected rows: " + result);
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }

        System.out.println("Goodbye!");
    }

    private static void printResultSet(ResultSet rs) {
        // Print column headers
        rs.getColumns().forEach(col -> System.out.printf("%-20s", col.name()));
        System.out.println();

        // Print separator
        rs.getColumns().forEach(col -> System.out.print("-".repeat(20)));
        System.out.println();

        // Print rows
        rs.getRows().forEach(row -> {
            rs.getColumns().forEach(col -> System.out.printf("%-20s", row.getValue(col.name())));
            System.out.println();
        });
        System.out.println("\nTotal rows: " + rs.getRowCount());
    }
} 