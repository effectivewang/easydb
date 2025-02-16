package com.easydb.sql.planner.expression;

import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.sql.parser.ParseTreeHelper;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * Builds expressions from parse trees, similar to PostgreSQL's expression parser.
 */
public class ExpressionBuilder {
    
    public static Expression build(ParseTree expr, List<RangeTableEntry> rangeTable) {
        return switch (expr.getType()) {
            case COLUMN_REF -> buildColumnRef(expr, rangeTable);
            case CONSTANT -> buildConstant(expr);
            case FUNCTION_CALL -> buildFunctionCall(expr, rangeTable);
            case ARITHMETIC_EXPR -> buildArithmeticExpr(expr, rangeTable);
            default -> throw new IllegalStateException(
                "Unsupported expression type: " + expr.getType());
        };
    }

    public static void processSelectItem(
            ParseTree selectItem,
            List<RangeTableEntry> rangeTable,
            List<String> targetList,
            List<String> sourceColumns,
            List<Integer> columnIndexes,
            List<Expression> expressions) {

        switch (selectItem.getType()) {
            case COLUMN_REF -> processColumnRef(
                selectItem, 
                rangeTable, 
                targetList, 
                sourceColumns, 
                columnIndexes
            );
            case STAR -> processStarExpansion(
                rangeTable, 
                targetList, 
                sourceColumns, 
                columnIndexes
            );
            case FUNCTION_CALL, ARITHMETIC_EXPR -> processExpression(
                selectItem,
                rangeTable, 
                targetList, 
                expressions
            );
            default -> throw new IllegalStateException(
                "Unsupported select item type: " + selectItem.getType());
        }
    }

    private static Expression buildColumnRef(ParseTree expr, List<RangeTableEntry> rangeTable) {
        String columnRef = expr.getValue();
        String[] parts = columnRef.split("\\.");
        
        RangeTableEntry rte;
        String columnName;
        
        if (parts.length == 2) {
            // Qualified column reference (e.g., "a.id")
            String tableAlias = parts[0];
            columnName = parts[1];
            
            rte = rangeTable.stream()
                .filter(r -> r.getAlias().equals(tableAlias))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                    "Invalid table alias: " + tableAlias));
        } else {
            // Unqualified column reference (e.g., "id")
            columnName = columnRef;
            List<RangeTableEntry> matches = rangeTable.stream()
                .filter(r -> r.hasColumn(columnName))
                .toList();

            if (matches.isEmpty()) {
                throw new IllegalStateException("Column not found: " + columnName);
            }
            if (matches.size() > 1) {
                throw new IllegalStateException("Ambiguous column reference: " + columnName);
            }
            rte = matches.get(0);
        }

        if (!rte.hasColumn(columnName)) {
            throw new IllegalStateException(
                "Column not found: " + columnName + " in table " + rte.getAlias());
        }

        return new Expression(
            ExpressionType.COLUMN_REF,
            rte.getQualifiedName(columnName)
        );
    }

    private static Expression buildConstant(ParseTree expr) {
        return new Expression(ExpressionType.CONSTANT, expr.getValue());
    }

    private static Expression buildFunctionCall(ParseTree functionCall, List<RangeTableEntry> rangeTable) {
        String functionName = functionCall.getValue();
        List<Expression> args = functionCall.getChildren().stream()
            .map(child -> build(child, rangeTable))
            .collect(Collectors.toList());

        return new Expression(
            ExpressionType.FUNCTION_CALL,
            functionName,
            args
        );
    }

    private static Expression buildArithmeticExpr(ParseTree arithmeticExpr, List<RangeTableEntry> rangeTable) {
        Expression left = build(arithmeticExpr.getChild(0), rangeTable);
        String operator = arithmeticExpr.getChild(1).getValue();
        Expression right = build(arithmeticExpr.getChild(2), rangeTable);

        return new Expression(
            ExpressionType.ARITHMETIC,
            operator,
            left,
            right
        );
    }

    private static void processColumnRef(
            ParseTree selectItem,
            List<RangeTableEntry> rangeTable,
            List<String> targetList,
            List<String> sourceColumns,
            List<Integer> columnIndexes) {
            
        String columnRef = selectItem.getValue();
        String[] parts = columnRef.split("\\.");
        
        RangeTableEntry rte;
        String columnName;
        
        if (parts.length == 2) {
            String tableAlias = parts[0];
            columnName = parts[1];
            rte = rangeTable.stream()
                .filter(r -> r.getAlias().equals(tableAlias))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                    "Invalid table alias: " + tableAlias));
        } else {
            columnName = columnRef;
            List<RangeTableEntry> matches = rangeTable.stream()
                .filter(r -> r.hasColumn(columnName))
                .toList();

            if (matches.isEmpty()) {
                throw new IllegalStateException("Column not found: " + columnName);
            }
            if (matches.size() > 1) {
                throw new IllegalStateException("Ambiguous column reference: " + columnName);
            }
            rte = matches.get(0);
        }

        ParseTree aliasNode = ParseTreeHelper.getChildOfType(selectItem, ParseTreeType.ALIAS);
        String alias = aliasNode == null ? null : aliasNode.getValue();
        int sourceIndex = rte.getColumnIndex(columnName);
        
        targetList.add(alias != null ? alias : columnName);
        sourceColumns.add(rte.getQualifiedName(columnName));
        columnIndexes.add(sourceIndex);
    }

    private static void processStarExpansion(
            List<RangeTableEntry> rangeTable,
            List<String> targetList,
            List<String> sourceColumns,
            List<Integer> columnIndexes) {
            
        for (RangeTableEntry rte : rangeTable) {
            List<String> columns = rte.getMetadata().columnNames();
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                String qualifiedName = rte.getQualifiedName(columnName);
                targetList.add(qualifiedName);
                sourceColumns.add(qualifiedName);
                columnIndexes.add(i);
            }
        }
    }

    private static void processExpression(
            ParseTree selectItem,
            List<RangeTableEntry> rangeTable,
            List<String> targetList,
            List<Expression> expressions) {
            
        Expression expr = build(selectItem, rangeTable);
        ParseTree aliasNode = ParseTreeHelper.getChildOfType(selectItem, ParseTreeType.ALIAS);
        String alias = aliasNode == null ? null : aliasNode.getValue();
        targetList.add(alias != null ? alias : expr.toString());
        expressions.add(expr);
    }
} 