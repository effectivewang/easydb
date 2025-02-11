package com.easydb.sql.ddl;

import com.easydb.core.Column;
import com.easydb.core.DataType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.constraint.*;
import java.util.*;

/**
 * Builds TableMetadata from a CREATE TABLE parse tree.
 * Follows PostgreSQL's pg_class and pg_attribute structure.
 */
public class TableMetadataBuilder {
    
    public static TableMetadata fromParseTree(ParseTree parseTree) {
        // Get table name
        ParseTree tableRef = findChildOfType(parseTree, ParseTreeType.TABLE_REF);
        if (tableRef == null) {
            throw new IllegalArgumentException("Missing table name in CREATE TABLE statement");
        }
        String tableName = tableRef.getValue();

        // Get column definitions
        ParseTree columnList = findChildOfType(parseTree, ParseTreeType.COLUMN_LIST);
        if (columnList == null) {
            throw new IllegalArgumentException("Missing column definitions in CREATE TABLE statement");
        }

        // Process columns
        List<Column> columns = new ArrayList<>();
        List<Constraint> constraints = new ArrayList<>();
        int constraintCounter = 0;

        for (ParseTree columnDef : columnList.getChildren()) {
            Column column = processColumnDefinition(columnDef);
            columns.add(column);

            // Handle column constraints
            ParseTree constraintList = findChildOfType(columnDef, ParseTreeType.CONSTRAINT_REF);
            // Handle constraints
            ParseTree parentTree = findChildOfType(columnDef, ParseTreeType.CONSTRAINT_REF);
            if (constraintList != null) {
                for (ParseTree constraintDef : constraintList.getChildren()) {
                    String constraintName = generateConstraintName(tableName, column.name(), 
                        constraintDef.getType(), constraintCounter++);
                    
                    Constraint constraint = createConstraint(constraintName, tableName, 
                        column.name(), constraintDef);
                    
                    if (constraint != null) {
                        constraints.add(constraint);
                    }
                }
            }
        }

        // Handle table-level constraints
        ParseTree tableConstraints = findChildOfType(parseTree, ParseTreeType.TABLE_CONSTRAINTS);
        if (tableConstraints != null) {
            for (ParseTree constraintDef : tableConstraints.getChildren()) {
                String constraintName = generateConstraintName(tableName, null, 
                    constraintDef.getType(), constraintCounter++);
                
                Constraint constraint = createTableConstraint(constraintName, tableName, 
                    constraintDef);
                
                if (constraint != null) {
                    constraints.add(constraint);
                }
            }
        }

        return new TableMetadata(
            tableName,
            columns,
            new HashMap<>(),
            constraints
        );
    }

    private static Constraint createConstraint(String name, String tableName, 
            String columnName, ParseTree constraintDef) {
        switch (constraintDef.getType()) {
            case PRIMARY_KEY_CONSTRAINT:
                return new PrimaryKeyConstraint(name, tableName, List.of(columnName));
                
            case FOREIGN_KEY_CONSTRAINT:
                ParseTree refTable = findChildOfType(constraintDef, ParseTreeType.TABLE_REF);
                ParseTree refColumn = findChildOfType(constraintDef, ParseTreeType.COLUMN_REF);
                return new ForeignKeyConstraint(
                    name, tableName, List.of(columnName),
                    refTable.getValue(), List.of(refColumn.getValue()),
                    ForeignKeyConstraint.FKAction.NO_ACTION,
                    ForeignKeyConstraint.FKAction.NO_ACTION
                );

            case UNIQUE_CONSTRAINT:
                return new UniqueConstraint(name, tableName, List.of(columnName));
                
            case CHECK_CONSTRAINT:
                ParseTree predicate = findChildOfType(constraintDef, ParseTreeType.CHECK_CONSTRAINT);
                return new CheckConstraint(name, tableName, List.of(columnName), new HashMap<>());
                
            default:
                throw new IllegalArgumentException("Unsupported constraint type: " + 
                    constraintDef.getType());
        }
    }

    private static Constraint createTableConstraint(String name, String tableName, 
            ParseTree constraintDef) {
        switch (constraintDef.getType()) {
            case PRIMARY_KEY_CONSTRAINT:
                ParseTree pkColumns = findChildOfType(constraintDef, ParseTreeType.COLUMN_LIST);
                return new PrimaryKeyConstraint(name, tableName, 
                    extractColumnNames(pkColumns));
                
            case FOREIGN_KEY_CONSTRAINT:
                ParseTree fkColumns = findChildOfType(constraintDef, ParseTreeType.COLUMN_LIST);
                ParseTree refTable = findChildOfType(constraintDef, ParseTreeType.TABLE_REF);
                ParseTree refColumns = findChildOfType(constraintDef, ParseTreeType.COLUMN_LIST);
                return new ForeignKeyConstraint(
                    name, tableName, extractColumnNames(fkColumns),
                    refTable.getValue(), extractColumnNames(refColumns),
                    ForeignKeyConstraint.FKAction.NO_ACTION,
                    ForeignKeyConstraint.FKAction.NO_ACTION
                );
                
            case UNIQUE_CONSTRAINT:
                ParseTree uniqueColumns = findChildOfType(constraintDef, ParseTreeType.COLUMN_LIST);
                return new UniqueConstraint(name, tableName, 
                    extractColumnNames(uniqueColumns));
                
            default:
                throw new IllegalArgumentException("Unsupported table constraint type: " + 
                    constraintDef.getType());
        }
    }

    private static String generateConstraintName(String tableName, String columnName, 
            ParseTreeType constraintType, int counter) {
        String prefix = switch (constraintType) {
            case PRIMARY_KEY_CONSTRAINT -> "pk";
            case FOREIGN_KEY_CONSTRAINT -> "fk";
            case UNIQUE_CONSTRAINT -> "uq";
            case CHECK_CONSTRAINT -> "ck";
            default -> "ct";
        };
        
        return String.format("%s_%s%s_%d", 
            prefix, 
            tableName.toLowerCase(), 
            columnName != null ? "_" + columnName.toLowerCase() : "",
            counter
        );
    }
    
    private static Column processColumnDefinition(ParseTree columnDef) {
        String name = columnDef.getValue();
        DataType type = parseDataType(columnDef.getChild(0).getType());
        return new Column(name, type);
    }

    private static DataType parseDataType(ParseTreeType type) {
        switch (type) {
            case INTEGER_TYPE:
                return DataType.INTEGER;
            case STRING_TYPE:
                return DataType.STRING;
            case BOOLEAN_TYPE:
                return DataType.BOOLEAN;
            case DOUBLE_TYPE:
                return DataType.DOUBLE;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + type);
        }
    }

    private static ParseTree findChildOfType(ParseTree parent, ParseTreeType type) {
        for (ParseTree child : parent.getChildren()) {
            if (child.getType() == type) {
                return child;
            }
        }
        return null;
    }

    private static List<String> extractColumnNames(ParseTree columnList) {
        if (columnList == null) {
            return List.of();
        }

        List<String> columnNames = new ArrayList<>();
        
        switch (columnList.getType()) {
            case COLUMN_LIST:
                // For table-level constraints with explicit column lists
                for (ParseTree columnRef : columnList.getChildren()) {
                    if (columnRef.getType() == ParseTreeType.COLUMN_REF) {
                        columnNames.add(columnRef.getValue());
                    } else {
                        columnNames.add(columnRef.getChild(0).getValue());
                    }
                }
                break;
            
            case COLUMN_REF:
                // For single column references
                columnNames.add(columnList.getValue());
                break;
                        
            default:
                throw new IllegalArgumentException(
                    "Unexpected parse tree node type for column list: " + columnList.getType());
        }
        
        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException("No column names found in column list");
        }
        
        return columnNames;
    }
}