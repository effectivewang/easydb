package com.easydb.sql.planner;

import com.easydb.storage.Storage;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.index.Index;
import com.easydb.index.HashIndex;
import com.easydb.index.IndexType;
import com.easydb.storage.Tuple;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import com.easydb.sql.planner.operation.IndexScanOperation;
import com.easydb.sql.planner.operation.SequentialScanOperation;
import com.easydb.sql.planner.operation.ProjectOperation;
import com.easydb.sql.planner.operation.InsertOperation;
import com.easydb.sql.planner.operation.FilterOperation;
import com.easydb.sql.planner.operation.UpdateOperation;
import com.easydb.sql.planner.operation.DeleteOperation;
import com.easydb.sql.planner.expression.ExpressionBuilder;
import com.easydb.sql.planner.expression.Expression;
import com.easydb.storage.metadata.IndexMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
/**
 * Generates a query execution plan (QueryTree) from a parse tree.
 */
public class QueryTreeGenerator {
    private final Storage storage;
    private final ExecutorService executorService;
    private final ParseTreeAnalyzer parseAnalyzer;  // New component for parse analysis

    private static final int DEFAULT_PARALLELISM = Runtime.getRuntime().availableProcessors();

    public QueryTreeGenerator(Storage storage) {
        this.storage = storage;
        this.executorService = Executors.newWorkStealingPool(DEFAULT_PARALLELISM);
        this.parseAnalyzer = new ParseTreeAnalyzer(storage);
    }

    public QueryTree generate(ParseTree parseTree) {
        QueryContext queryContext = parseAnalyzer.analyze(parseTree);
        switch (parseTree.getType()) {
            case SELECT_STATEMENT:
                return generateSelectTree(parseTree, queryContext);
            case INSERT_STATEMENT:
                return generateInsertTree(parseTree, queryContext);
            case UPDATE_STATEMENT:
                return generateUpdateTree(parseTree, queryContext);
            case DELETE_STATEMENT:
                return generateDeleteTree(parseTree, queryContext);
            default:
                throw new IllegalArgumentException("Unsupported statement type: " + parseTree.getType());
        }
    }

    private QueryTree generateSelectTree(ParseTree parseTree, QueryContext queryContext) {
        QueryTree result = null;
        
        // Process FROM clause first to get base tables
        ParseTree fromClause = findChildOfType(parseTree, ParseTreeType.FROM_CLAUSE);
        if (fromClause != null) {
            result = generateFromTree(fromClause, queryContext);
        }

        // Apply WHERE clause filters and decide on index usage
        ParseTree whereClause = findChildOfType(parseTree, ParseTreeType.WHERE_CLAUSE);
        if (whereClause != null) {
            QueryPredicate predicate = generatePredicate(whereClause.getChild(0), queryContext);
            result = optimizeAccessPath(result, predicate, queryContext);
        }

        // Handle GROUP BY
        ParseTree groupByClause = findChildOfType(parseTree, ParseTreeType.GROUP_BY_CLAUSE);
        if (groupByClause != null) {
            result = addGroupBy(result, groupByClause);
            
            // Apply HAVING filters
            ParseTree havingClause = findChildOfType(parseTree, ParseTreeType.HAVING_CLAUSE);
            if (havingClause != null) {
                result = addFilter(result, havingClause.getChild(0), queryContext);
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

    private QueryTree optimizeAccessPath(QueryTree scanNode, QueryPredicate predicate, QueryContext queryContext) {
        if (scanNode.getOperator() != QueryOperator.SEQUENTIAL_SCAN) {
            // Only optimize base table scans
            return addFilter(scanNode, predicate, queryContext);
        }

        RangeTableEntry rte = queryContext.resolveColumn(scanNode.getOutputColumns().get(0));
        TableMetadata metadata = rte.getMetadata();

        // Check if we can use an index for this predicate
        if (shouldUseIndexScan(metadata, predicate)) {
            // Replace sequential scan with index scan
            QueryTree indexScan = createIndexScan(metadata, rte.getTableName(), predicate, queryContext.getRangeTable());
            return indexScan;
        }

        // Fall back to filter on sequential scan
        return addFilter(scanNode, predicate, queryContext);
    }

    private boolean shouldUseIndexScan(TableMetadata metadata, QueryPredicate predicate) {
        // Only consider simple equality predicates for index scans
        if (predicate.getType() != QueryPredicate.PredicateType.EQUALS) {
            return false;
        }

        String columnName = predicate.getColumn();
        if (columnName == null) {
            return false;
        }

        // Check if there's a usable index
        return metadata.indexes().values().stream()
            .anyMatch(index -> {
                // For now, only consider single-column indexes
                return index.columnNames().size() == 1 &&
                       index.hasColumn(columnName) &&
                       (index.type() == IndexType.HASH || 
                        index.type() == IndexType.BTREE);
            });
    }

    private QueryTree generateFromTree(ParseTree fromClause, QueryContext queryContext) {
        List<QueryTree> scans = new ArrayList<>();
        List<RangeTableEntry> rangeTable = queryContext.getRangeTable();

        // Create scan nodes using existing range table entries
        for (ParseTree tableRef : fromClause.getChildren()) {
            String tableName = getTableName(tableRef);
            RangeTableEntry rte = findRangeTableEntry(rangeTable, tableName);
            QueryTree scan = createSequentialScan(rte);
            scans.add(scan);
        }

        return scans.size() == 1 ? scans.get(0) : createJoinTree(scans, rangeTable);
    }
    private RangeTableEntry findRangeTableEntry(List<RangeTableEntry> rangeTable, String tableNameOrAlias) {
        return rangeTable.stream()
            .filter(rte -> tableNameOrAlias.equals(rte.getDisplayName()) || 
                          tableNameOrAlias.equals(rte.getTableName()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Table not found in range table: " + tableNameOrAlias));
    }

    private QueryTree createSequentialScan(RangeTableEntry rte) {
        List<String> qualifiedColumns = rte.getMetadata().columnNames().stream()
            .map(rte::getQualifiedName)
            .collect(Collectors.toList());

        return new QueryTree(
            QueryOperator.SEQUENTIAL_SCAN,
            new SequentialScanOperation(rte, null),  // No predicate initially
            qualifiedColumns,
            Arrays.asList(rte)
        );
    }

    private QueryTree createIndexScan(
            TableMetadata metadata, 
            String tableName, 
            QueryPredicate predicate, 
            List<RangeTableEntry> rangeTable) {
        
        // Find the appropriate index for this predicate
        IndexMetadata indexMetadata = findBestIndex(metadata, predicate);
        if (indexMetadata == null) {
            throw new IllegalStateException("No suitable index found for predicate");
        }

        // Split predicate into index condition and filter condition
        QueryPredicate indexCondition = extractIndexCondition(predicate, indexMetadata);
        QueryPredicate filterPredicate = extractRemainingPredicate(predicate, indexCondition);

        // Find the RTE for this table
        RangeTableEntry rte = findRangeTableEntry(rangeTable, tableName);

        return new QueryTree(
            QueryOperator.INDEX_SCAN,
            new IndexScanOperation(rte, indexMetadata, indexCondition, filterPredicate),
            metadata.columnNames(),
            rangeTable
        );
    }

    private IndexMetadata findBestIndex(TableMetadata metadata, QueryPredicate predicate) {
        // Find an index that matches the predicate columns
        return metadata.indexes().values().stream()
            .filter(index -> isIndexUsable(index, predicate))
            .findFirst()
            .orElse(null);
    }

    private boolean isIndexUsable(IndexMetadata index, QueryPredicate predicate) {
        // For now, only consider single-column indexes and equality predicates
        if (predicate.getPredicateType() != QueryPredicate.PredicateType.EQUALS) {
            return false;
        }

        String columnName = predicate.getColumn();
        return index.columnNames().size() == 1 &&
               index.hasColumn(columnName) &&
               (index.type() == IndexType.HASH || index.type() == IndexType.BTREE);
    }

    private QueryPredicate extractIndexCondition(QueryPredicate predicate, IndexMetadata indexMetadata) {
        if (predicate.getPredicateType() == QueryPredicate.PredicateType.AND) {
            // For compound predicates, extract the parts that can use the index
            return predicate.getSubPredicates().stream()
                .filter(subPred -> isIndexUsable(indexMetadata, subPred))
                .findFirst()
                .orElse(null);
        }
        
        // For simple predicates, use it directly if it matches the index
        return isIndexUsable(indexMetadata, predicate) ? predicate : null;
    }

    private QueryPredicate extractRemainingPredicate(QueryPredicate original, QueryPredicate indexCondition) {
        if (original == indexCondition) {
            return null;  // No remaining predicate
        }
        
        if (original.getPredicateType() == QueryPredicate.PredicateType.AND) {
            // Remove the index condition from AND predicate
            List<QueryPredicate> remaining = original.getSubPredicates().stream()
                .filter(subPred -> !subPred.equals(indexCondition))
                .collect(Collectors.toList());
            
            if (remaining.isEmpty()) {
                return null;
            } else if (remaining.size() == 1) {
                return remaining.get(0);
            } else {
                return QueryPredicate.and(remaining);
            }
        }
        
        return original;  // Keep original predicate if not used for index
    }

    private QueryTree createJoinTree(List<QueryTree> scans, List<RangeTableEntry> rangeTable) {
        // Start with the first two tables
        QueryTree result = createHashJoin(scans.get(0), scans.get(1), rangeTable);
        
        // Add remaining tables
        for (int i = 2; i < scans.size(); i++) {
            result = createHashJoin(result, scans.get(i), rangeTable);
        }
        
        return result;
    }

    private QueryTree createHashJoin(QueryTree left, QueryTree right,  List<RangeTableEntry> rangeTable) {
        List<String> combinedColumns = new ArrayList<>(left.getOutputColumns());
        combinedColumns.addAll(right.getOutputColumns());

        QueryTree join = new QueryTree(
            QueryOperator.HASH_JOIN,
            null,
            combinedColumns,
            rangeTable  // Use complete range table
        );
        
        join.addChild(left);
        join.addChild(right);
        
        estimateJoinCost(join);
        return join;
    }

    private QueryTree addFilter(QueryTree input, ParseTree filterExpr, QueryContext queryContext) {
        QueryPredicate predicate = generatePredicate(filterExpr, queryContext);
        return addFilter(input, predicate, queryContext);
    }
    
    private QueryTree addFilter(QueryTree input, QueryPredicate predicate, QueryContext queryContext) {
        QueryTree filter = new QueryTree(
            QueryOperator.FILTER,
            new FilterOperation(predicate, input.getRangeTable().get(0)),
            input.getOutputColumns(),
            input.getRangeTable()
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
            input.getOutputColumns(),
            input.getRangeTable()
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
            input.getOutputColumns(),
            input.getRangeTable()
        );
        aggregate.addChild(input);
        return aggregate;
    }

    private QueryTree addProjection(QueryTree input, ParseTree selectList) {
        List<String> targetList = new ArrayList<>();
        List<String> sourceColumns = new ArrayList<>();
        List<Integer> columnIndexes = new ArrayList<>();
        List<Expression> expressions = new ArrayList<>();

        // Use ExpressionBuilder to process select items
        for (ParseTree selectItem : selectList.getChildren()) {
            ExpressionBuilder.processSelectItem(
                selectItem,
                input.getRangeTable(),
                targetList,
                sourceColumns,
                columnIndexes,
                expressions
            );
        }

        ProjectOperation operation = new ProjectOperation(
            targetList,
            sourceColumns,
            columnIndexes,
            expressions,
            input.getRangeTable()
        );

        QueryTree result = new QueryTree(
            QueryOperator.PROJECT,
            operation,
            targetList,
            input.getRangeTable()
        );
        result.addChild(input);
        return result;
    }
    
    private List<String> extractProjectionColumns(ParseTree selectList) {
        List<String> columns = new ArrayList<>();
        for (ParseTree column : selectList.getChildren()) {
            columns.add(column.getValue());
        }
        return columns;
    }
    

    private QueryPredicate generatePredicate(ParseTree expr, QueryContext queryContext) {
        switch (expr.getType()) {
            case BINARY_EXPR:
                return generateBinaryPredicate(expr, queryContext);
            case COLUMN_REF:
                return generateColumnPredicate(expr, queryContext);
            default:
                throw new IllegalArgumentException("Unsupported expression type: " + expr.getType());
        }
    }

    private QueryPredicate generateBinaryPredicate(ParseTree expr, QueryContext queryContext) {
        ParseTree left = expr.getChild(0);
        ParseTree operator = expr.getChild(1);
        ParseTree right = expr.getChild(2);

        // Convert operator type to predicate type
        switch (operator.getType()) {
            case EQUALS_OPERATOR:
                return QueryPredicate.equals(left.getValue(), parseValue(right));
            case NOT_EQUALS_OPERATOR:
                return QueryPredicate.notEquals(left.getValue(), parseValue(right));
            case LESS_THAN_OPERATOR:
                return QueryPredicate.lessThan(left.getValue(), parseValue(right));
            case GREATER_THAN_OPERATOR:
                return QueryPredicate.greaterThan(left.getValue(), parseValue(right));
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

    private QueryPredicate generateColumnPredicate(ParseTree expr, QueryContext queryContext) {
        // This would handle column references in predicates
        return null;
    }

    public void shutdown() {
        executorService.shutdown();
    }
    
    private QueryTree generateInsertTree(ParseTree parseTree, QueryContext queryContext) {
        // Get table name
        ParseTree tableRef = findChildOfType(parseTree, ParseTreeType.TABLE_REF);
        if (tableRef == null) {
            throw new IllegalArgumentException("Missing table name in INSERT statement");
        }

        RangeTableEntry rte = findRangeTableEntry(
            queryContext.getRangeTable(), 
            getTableName(tableRef)
        );

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
            new InsertOperation(rte, columns, allValues),
            outputColumns,
            queryContext.getRangeTable()
        );
        

        // Set cost estimates
        TableMetadata metadata = rte.getMetadata();
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

    private QueryTree generateUpdateTree(ParseTree parseTree, QueryContext queryContext) {
        // 1. Process target table
        ParseTree targetTable = findChildOfType(parseTree, ParseTreeType.TABLE_REF);
        RangeTableEntry targetRte = findRangeTableEntry(
            queryContext.getRangeTable(), 
            getTableName(targetTable)
        );

        // Process WHERE clause
        ParseTree whereClause = findChildOfType(parseTree, ParseTreeType.WHERE_CLAUSE);
        Expression whereExpr = whereClause != null ? 
            ExpressionBuilder.build(whereClause.getChild(0), queryContext.getRangeTable()) : null;

        // Process SET clause
        ParseTree setClause = findChildOfType(parseTree, ParseTreeType.SET_CLAUSE);
        List<String> targetColumns = new ArrayList<>();
        List<Expression> setExpressions = new ArrayList<>();
        
        for (ParseTree assignment : setClause.getChildren()) {
            targetColumns.add(assignment.getChild(0).getValue());
            setExpressions.add(
                ExpressionBuilder.build(assignment.getChild(1), queryContext.getRangeTable())
            );
        }

        // Build scan and filter plan
        QueryTree result = generateScanAndFilter(
            parseTree, 
            targetRte, 
            queryContext
        );

        // Add UPDATE node on top with the where expression
        QueryTree updateNode = new QueryTree(
            QueryOperator.UPDATE,
            new UpdateOperation(
                targetColumns, 
                setExpressions, 
                targetRte,
                whereExpr  // Pass the where expression
            ),
            List.of(),
            queryContext.getRangeTable()
        );
        updateNode.addChild(result);

        return updateNode;
    }

    private QueryTree generateDeleteTree(ParseTree parseTree, QueryContext queryContext) {
        // 1. Process target table
        ParseTree targetTable = findChildOfType(parseTree, ParseTreeType.TABLE_REF);
        RangeTableEntry targetRte = findRangeTableEntry(
            queryContext.getRangeTable(), 
            getTableName(targetTable)
        );

        // 2. Process WHERE clause
        ParseTree whereClause = findChildOfType(parseTree, ParseTreeType.WHERE_CLAUSE);
        Expression whereExpr = whereClause != null ? 
            ExpressionBuilder.build(whereClause.getChild(0), queryContext.getRangeTable()) : null;

        // 3. Build scan and filter plan
        QueryTree result = generateScanAndFilter(
            parseTree, 
            targetRte, 
            queryContext
        );

        // 4. Add DELETE node on top with the where expression
        QueryTree deleteNode = new QueryTree(
            QueryOperator.DELETE,
            new DeleteOperation(targetRte, whereExpr),  // Pass the where expression
            List.of(),  // DELETE produces no output
            queryContext.getRangeTable()
        );
        deleteNode.addChild(result);

        return deleteNode;
    }

    // Helper method for common scan+filter logic
    private QueryTree generateScanAndFilter(
            ParseTree parseTree, 
            RangeTableEntry targetRte,
            QueryContext queryContext) {
            
        // 1. Create base sequential scan
        QueryTree result = createSequentialScan(targetRte);

        // 2. Add filter if WHERE clause exists
        ParseTree whereClause = findChildOfType(parseTree, ParseTreeType.WHERE_CLAUSE);
        if (whereClause != null) {
            QueryPredicate predicate = generatePredicate(whereClause.getChild(0), queryContext);
            result = optimizeAccessPath(result, predicate, queryContext);
        }

        return result;
    }
} 