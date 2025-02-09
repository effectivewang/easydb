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
        ParseTree columnList = findChildOfType(parseTree, ParseTreeType.COLUMN_REF);
        if (columnList == null) {
            throw new IllegalArgumentException("Missing column definitions in CREATE TABLE statement");
        }

        // Process columns
        List<Column> columns = new ArrayList<>();
        Map<String, Constraint> constraints = new HashMap<>();
        
        for (ParseTree columnDef : columnList.getChildren()) {
            Column column = processColumnDefinition(columnDef);
            columns.add(column);

            // Handle column constraints
            ParseTree constraintList = findChildOfType(columnDef, ParseTreeType.CONSTRAINTS);
            // Handle constraints
            ParseTree parentTree = findChildOfType(columnDef, ParseTreeType.CONSTRAINTS);
            if (constraintList != null) {
                for (ParseTree constraintDef : constraintList.getChildren()) {
                    String constraintName = generateConstraintName(tableName, column.name(), 
                        constraintDef.getType(), constraintCounter++);
                    
                    Constraint constraint = createConstraint(constraintName, tableName, 
                        column.name(), constraintDef);
                    
                    if (constraint != null) {
                        constraints.put(constraintName, constraint);
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
                    constraints.put(constraintName, constraint);
                }
            }
        }

        return new TableMetadata(
            tableName,
            columns,
            primaryKeyColumn,
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
                
            case NOT_NULL_CONSTRAINT:
                return new NotNullConstraint(name, tableName, List.of(columnName));
                
            case UNIQUE_CONSTRAINT:
                return new UniqueConstraint(name, tableName, List.of(columnName));
                
            case CHECK_CONSTRAINT:
                ParseTree predicate = findChildOfType(constraintDef, ParseTreeType.PREDICATE);
                return new CheckConstraint(name, tableName, List.of(columnName), 
                    convertToQueryPredicate(predicate));
                
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
                ParseTree refColumns = findChildOfType(constraintDef, ParseTreeType.REF_COLUMN_LIST);
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
            case NOT_NULL_CONSTRAINT -> "nn";
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
        String name = columnDef.getChild(0).getValue();
        DataType type = parseDataType(columnDef.getChild(1).getValue());
        return new Column(name, type);
    }

    private static DataType parseDataType(String typeStr) {
        return DataType.parse(typeStr);
    }

    private static ParseTree findChildOfType(ParseTree parent, ParseTreeType type) {
        for (ParseTree child : parent.getChildren()) {
            if (child.getType() == type) {
                return child;
            }
        }
        return null;
    }
}