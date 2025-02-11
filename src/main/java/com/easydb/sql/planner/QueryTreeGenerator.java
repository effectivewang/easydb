package com.easydb.sql.planner;

import com.easydb.storage.Storage;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.index.Index;
import com.easydb.index.HashIndex;
import com.easydb.index.IndexType;
import com.easydb.storage.Tuple;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Generates a query execution plan (QueryTree) from a parse tree.
 */
public class QueryTreeGenerator {
    private final Storage storage;
    private final ExecutorService executorService;
    private static final int DEFAULT_PARALLELISM = Runtime.getRuntime().availableProcessors();

    public QueryTreeGenerator(Storage storage) {
        this.storage = storage;
        this.executorService = Executors.newWorkStealingPool(DEFAULT_PARALLELISM);
    }

    public QueryTree generate(ParseTree parseTree) {
        switch (parseTree.getType()) {
            case SELECT_STATEMENT:
                return generateSelectTree(parseTree);
            case INSERT_STATEMENT:
                return generateInsertTree(parseTree);
            default:
                throw new IllegalArgumentException("Unsupported statement type: " + parseTree.getType());
        }
    }

    private QueryTree generateSelectTree(ParseTree parseTree) {
        QueryTree result = null;
        
        // Process FROM clause first to get base tables
        ParseTree fromClause = findChildOfType(parseTree, ParseTreeType.FROM_CLAUSE);
        if (fromClause != null) {
            result = generateFromTree(fromClause);
        }

        // Apply WHERE clause filters
        ParseTree whereClause = findChildOfType(parseTree, ParseTreeType.WHERE_CLAUSE);
        if (whereClause != null) {
            result = addFilter(result, whereClause.getChild(0));
        }

        // Handle GROUP BY
        ParseTree groupByClause = findChildOfType(parseTree, ParseTreeType.GROUP_BY_CLAUSE);
        if (groupByClause != null) {
            result = addGroupBy(result, groupByClause);
            
            // Apply HAVING filters
            ParseTree havingClause = findChildOfType(parseTree, ParseTreeType.HAVING_CLAUSE);
            if (havingClause != null) {
                result = addFilter(result, havingClause.getChild(0));
            }
        }

        // Apply ORDER BY
        ParseTree orderByClause = findChildOfType(parseTree, ParseTreeType.ORDER_BY_CLAUSE);
        if (orderByClause != null) {
            result = addSort(result, orderByClause);
        }

        // Finally, handle SELECT list projection
        ParseTree selectList = findChildOfType(parseTree, ParseTreeType.SELECT_LIST);
        if (selectList != null) {
            result = addProjection(result, selectList);
        }

        return result;
    }

    private QueryTree generateFromTree(ParseTree fromClause) {
        List<QueryTree> scans = new ArrayList<>();
        
        // Generate scan nodes for each table
        for (ParseTree tableRef : fromClause.getChildren()) {
            String tableName = getTableName(tableRef);
            TableMetadata metadata = storage.getTableMetadata(tableName);
            
            // Choose between sequential scan and index scan
            QueryTree scan = shouldUseIndexScan(metadata, null) ? 
                createIndexScan(metadata, tableName, null) :
                createSequentialScan(metadata, tableName);
                
            scans.add(scan);
        }

        // If there's only one table, return its scan
        if (scans.size() == 1) {
            return scans.get(0);
        }

        // Otherwise, create join trees
        return createJoinTree(scans);
    }


    private QueryTree createJoinTree(List<QueryTree> scans) {
        // Start with the first two tables
        QueryTree result = createHashJoin(scans.get(0), scans.get(1));
        
        // Add remaining tables
        for (int i = 2; i < scans.size(); i++) {
            result = createHashJoin(result, scans.get(i));
        }
        
        return result;
    }

    private QueryTree createHashJoin(QueryTree left, QueryTree right) {
        // In a real implementation, we would determine join conditions and choose
        // between different join algorithms (hash join, merge join, nested loop)
        QueryTree join = new QueryTree(
            QueryOperator.HASH_JOIN,
            null,  // Join condition would go here
            combineOutputColumns(left.getOutputColumns(), right.getOutputColumns())
        );
        
        join.addChild(left);
        join.addChild(right);
        
        // Set cost estimates
        estimateJoinCost(join);
        
        return join;
    }

    private QueryTree addFilter(QueryTree input, ParseTree filterExpr) {
        QueryPredicate predicate = generatePredicate(filterExpr);
        QueryTree filter = new QueryTree(
            QueryOperator.FILTER,
            predicate,
            input.getOutputColumns()
        );
        filter.addChild(input);
        return filter;
    }

    private QueryTree addSort(QueryTree input, ParseTree orderByClause) {
        // Extract sort columns and create sort node
        List<String> sortColumns = extractSortColumns(orderByClause);
        QueryTree sort = new QueryTree(
            QueryOperator.SORT,
            null,
            input.getOutputColumns()
        );
        sort.addChild(input);
        return sort;
    }

    private QueryTree addGroupBy(QueryTree input, ParseTree groupByClause) {
        // Extract group by columns and create aggregate node
        List<String> groupColumns = extractGroupColumns(groupByClause);
        QueryTree aggregate = new QueryTree(
            QueryOperator.HASH_AGGREGATE,
            null,
            input.getOutputColumns()
        );
        aggregate.addChild(input);
        return aggregate;
    }

    private QueryTree addProjection(QueryTree input, ParseTree selectList) {
        List<String> columns = extractProjectionColumns(selectList);
        QueryTree projection = new QueryTree(
            QueryOperator.PROJECT,
            null,
            columns
        );
        projection.addChild(input);
        return projection;
    }
    
    private List<String> extractProjectionColumns(ParseTree selectList) {
        List<String> columns = new ArrayList<>();
        for (ParseTree column : selectList.getChildren()) {
            columns.add(column.getValue());
        }
        return columns;
    }
    

    private QueryPredicate generatePredicate(ParseTree expr) {
        switch (expr.getType()) {
            case BINARY_EXPR:
                return generateBinaryPredicate(expr);
            case COLUMN_REF:
                return generateColumnPredicate(expr);
            default:
                throw new IllegalArgumentException("Unsupported expression type: " + expr.getType());
        }
    }

    private QueryPredicate generateBinaryPredicate(ParseTree expr) {
        ParseTree left = expr.getChild(0);
        ParseTree operator = expr.getChild(1);
        ParseTree right = expr.getChild(2);

        // Convert operator type to predicate type
        switch (operator.getType()) {
            case EQUALS_OPERATOR:
                return QueryPredicate.equals(left.getValue(), right.getValue());
            case NOT_EQUALS_OPERATOR:
                return QueryPredicate.notEquals(left.getValue(), right.getValue());
            case LESS_THAN_OPERATOR:
                return QueryPredicate.lessThan(left.getValue(), right.getValue());
            case GREATER_THAN_OPERATOR:
                return QueryPredicate.greaterThan(left.getValue(), right.getValue());
            default:
                throw new IllegalArgumentException("Unsupported operator type: " + operator.getType());
        }
    }

    private ParseTree findChildOfType(ParseTree tree, ParseTreeType type) {
        for (ParseTree child : tree.getChildren()) {
            if (child.getType() == type) {
                return child;
            }
        }
        return null;
    }

    private List<String> combineOutputColumns(List<String> left, List<String> right) {
        List<String> combined = new ArrayList<>(left);
        combined.addAll(right);
        return combined;
    }

    private void estimateJoinCost(QueryTree join) {
        // In a real implementation, this would use statistics to estimate cost
        QueryTree left = join.getChildren().get(0);
        QueryTree right = join.getChildren().get(1);
        join.setEstimatedRows(left.getEstimatedRows() * right.getEstimatedRows());
        join.setEstimatedCost(left.getEstimatedCost() + right.getEstimatedCost() + join.getEstimatedRows());
    }

    private boolean shouldUseIndexScan(TableMetadata metadata, QueryPredicate predicate) {
        // Check if there's an index that can be used for this predicate
        if (predicate.getColumn() == null) {
            return false;
        }
        String columnName = predicate.getColumn();
        return metadata.hasIndex(columnName) && 
               metadata.getIndex(columnName).type().equals(IndexType.HASH);
    }

    private QueryTree createSequentialScan(TableMetadata metadata, String tableName) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private QueryTree createIndexScan(TableMetadata metadata, String tableName, QueryPredicate predicate) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private String getTableName(ParseTree tableRef) {
        return tableRef.getType() == ParseTreeType.ALIAS ? 
            tableRef.getChild(0).getValue() : tableRef.getValue();
    }

    private List<String> extractSortColumns(ParseTree orderByClause) {
        List<String> columns = new ArrayList<>();
        for (ParseTree column : orderByClause.getChildren()) {
            columns.add(column.getValue());
        }
        return columns;
    }

    private List<String> extractGroupColumns(ParseTree groupByClause) {
        List<String> columns = new ArrayList<>();
        for (ParseTree column : groupByClause.getChildren()) {
            columns.add(column.getValue());
        }
        return columns;
    }

    private QueryPredicate generateColumnPredicate(ParseTree expr) {
        // This would handle column references in predicates
        return null;
    }

    public void shutdown() {
        executorService.shutdown();
    }

    private List<Tuple> executeIndexScan(QueryTree node) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void executeCreateIndex(QueryTree node) {
        String tableName = extractTableName(node);
        String columnName = extractColumnName(node);
        String indexType = extractIndexType(node);
        
        throw new UnsupportedOperationException("Not Implemented!");
    }

    private String extractTableName(QueryTree node) {
        throw new UnsupportedOperationException("Not Implemented!");
    }

    private String extractColumnName(QueryTree node) {
        throw new UnsupportedOperationException("Not Implemented!");
    }

    private String extractIndexType(QueryTree node) {
        throw new UnsupportedOperationException("Not Implemented!");
    }
    
    private QueryTree generateInsertTree(ParseTree parseTree) {
        // Get table name
        ParseTree tableRef = findChildOfType(parseTree, ParseTreeType.TABLE_REF);
        if (tableRef == null) {
            throw new IllegalArgumentException("Missing table name in INSERT statement");
        }
        String tableName = tableRef.getValue();

        // Get column list
        ParseTree columnList = findChildOfType(parseTree, ParseTreeType.LIST);
        List<String> columns = new ArrayList<>();
        if (columnList != null) {
            for (ParseTree column : columnList.getChildren()) {
                columns.add(column.getValue());
            }
        }
        // Get values list (multiple rows)
        ParseTree valuesClause = findChildOfType(parseTree, ParseTreeType.VALUES_CLAUSE);
        List<List<Object>> allValues = new ArrayList<>();
        // Process each row of values
        for (ParseTree valueList : valuesClause.getChildren()) {
            List<Object> rowValues = new ArrayList<>();
            for (ParseTree value : valueList.getChildren()) {
                rowValues.add(parseValue(value));
            }
            allValues.add(rowValues);
        }

        // Create output columns for result
        List<String> outputColumns = new ArrayList<>();
        outputColumns.add("affected_rows");

        // Create INSERT operator node with InsertOperation
        QueryTree insertNode = new QueryTree(
            QueryOperator.INSERT,
            new InsertOperation(tableName, columns, allValues),
            outputColumns
        );
        

        // Set cost estimates
        TableMetadata metadata = storage.getTableMetadata(tableName);
        double baseCost = 1.0 * allValues.size(); // Base cost for insertion
        double indexCost = metadata.indexes() != null ? metadata.indexes().size() * 0.5 : 0; // Additional cost per index
        insertNode.setEstimatedCost(baseCost + indexCost);
        insertNode.setEstimatedRows(allValues.size()); // INSERT returns number of affected rows

        return insertNode;
    }

    private Object parseValue(ParseTree value) {
        switch (value.getType()) {
            case INTEGER_TYPE:
                return Integer.parseInt(value.getValue());
            case STRING_TYPE:
                return value.getValue();
            case BOOLEAN_TYPE:
                return Boolean.parseBoolean(value.getValue());
            case DOUBLE_TYPE:
                return Double.parseDouble(value.getValue());
            case NULL_TYPE:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported value type: " + value.getType());
        }
    }


} 