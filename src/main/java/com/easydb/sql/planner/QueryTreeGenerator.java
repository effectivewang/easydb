package com.easydb.sql.planner;

import com.easydb.storage.Storage;
import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.metadata.IndexMetadata;
import com.easydb.storage.Tuple;
import com.easydb.storage.transaction.Transaction;
import com.easydb.index.Index;
import com.easydb.index.HashIndex;
import com.easydb.index.IndexType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import com.easydb.sql.planner.operation.*;
import com.easydb.sql.planner.expression.Expression;
import com.easydb.sql.planner.expression.ExpressionBuilder;
import com.easydb.sql.planner.expression.TypeConverter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.Optional;
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
            Expression predicate = generatePredicate(whereClause.getChild(0), queryContext);
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

    private QueryTree optimizeAccessPath(QueryTree scanNode, Expression predicate, QueryContext queryContext) {
        if (scanNode.getOperator() != QueryOperator.SEQUENTIAL_SCAN) {
            // Only optimize base table scans
            return addFilter(scanNode, predicate, queryContext);
        }

        RangeTableEntry rte = queryContext.resolveColumn(scanNode.getOutputColumns().get(0));
        TableMetadata metadata = rte.getMetadata();

        // Check if we can use an index for this predicate
        IndexMetadata bestIndex = findBestIndex(metadata, predicate);
        if (bestIndex != null) {
            // Replace sequential scan with index scan
            QueryTree indexScan = createIndexScan(metadata, rte.getTableName(), predicate, queryContext.getRangeTable());
            
            // Estimate cost for the index scan
            double rowsPerKey = 1.0; // Assume unique index by default
            if (!bestIndex.isUnique()) {
                // For non-unique indexes, estimate 10% of table rows per key
                rowsPerKey = Math.max(1, metadata.estimatedRows() * 0.1);
            }
            
            // Set cost and row estimates
            indexScan.setEstimatedRows((long)rowsPerKey);
            indexScan.setEstimatedCost(rowsPerKey * 1.0); // Index lookup cost
            
            return indexScan;
        }

        // Fall back to filter on sequential scan
        QueryTree filteredScan = addFilter(scanNode, predicate, queryContext);
        
        // Estimate selectivity for the filter (default to 30% of rows passing)
        double selectivity = estimateSelectivity(predicate, metadata);
        double estimatedRows = metadata.estimatedRows() * selectivity;
        
        filteredScan.setEstimatedRows((long)estimatedRows);
        filteredScan.setEstimatedCost(metadata.estimatedRows() + estimatedRows); // Full scan + filter cost
        
        return filteredScan;
    }

    /**
     * Estimates the selectivity of a predicate (what fraction of rows will pass)
     */
    private double estimateSelectivity(Expression predicate, TableMetadata metadata) {
        if (predicate == null) return 1.0;
        
        switch (predicate.getType()) {
            case COMPARISON:
                switch (predicate.getOperator()) {
                    case EQUALS:
                        return 0.1; // Assume 10% selectivity for equality
                    case NOT_EQUALS:
                        return 0.9; // Assume 90% selectivity for inequality
                    case LESS_THAN:
                    case GREATER_THAN:
                        return 0.3; // Assume 30% selectivity for range conditions
                    case LESS_EQUAL:
                    case GREATER_EQUAL:
                        return 0.4; // Assume 40% selectivity for inclusive range
                    default:
                        return 0.3;
                }
            case LOGICAL:
                switch (predicate.getOperator()) {
                    case AND:
                        return estimateSelectivity(predicate.getLeft(), metadata) * 
                               estimateSelectivity(predicate.getRight(), metadata);
                    case OR:
                        return Math.min(1.0, 
                            estimateSelectivity(predicate.getLeft(), metadata) + 
                            estimateSelectivity(predicate.getRight(), metadata));
                    default:
                        return 0.3;
                }
            case NOT:
                return 1.0 - estimateSelectivity(predicate.getLeft(), metadata);
            default:
                return 0.3;
        }
    }

    private boolean shouldUseIndexScan(TableMetadata metadata, Expression predicate) {
        // Check if there's a suitable index for this predicate
        return findBestIndex(metadata, predicate) != null;
    }

    private IndexMetadata findBestIndex(TableMetadata metadata, Expression predicate) {
        // No indexes available
        if (metadata.indexes().isEmpty()) {
            return null;
        }
        
        // For compound predicates, try to find indexes for the components
        if (predicate.getType() == Expression.ExpressionType.LOGICAL && 
            predicate.getOperator() == Expression.Operator.AND) {
            // Try left subtree first
            IndexMetadata leftIndex = findBestIndex(metadata, predicate.getLeft());
            if (leftIndex != null) {
                return leftIndex;
            }
            // Try right subtree
            return findBestIndex(metadata, predicate.getRight());
        }
        
        // Find all usable indexes for this predicate
        List<IndexMetadata> usableIndexes = metadata.indexes().values().stream()
            .filter(index -> isIndexUsable(index, predicate))
            .collect(Collectors.toList());
        
        if (usableIndexes.isEmpty()) {
            return null;
        }
        
        // Prefer unique indexes over non-unique
        Optional<IndexMetadata> uniqueIndex = usableIndexes.stream()
            .filter(IndexMetadata::isUnique)
            .findFirst();
        
        if (uniqueIndex.isPresent()) {
            return uniqueIndex.get();
        }
        
        // Otherwise, return the first usable index
        return usableIndexes.get(0);
    }

    private boolean isIndexUsable(IndexMetadata index, Expression predicate) {
        if (predicate.getType() != Expression.ExpressionType.COMPARISON) {
            return false;
        }

        String column = predicate.getLeft().getValue().toString();
        
        switch (predicate.getOperator()) {
            case EQUALS:
            case NOT_EQUALS:
                return index.columns().contains(column);
                
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_EQUAL:
            case GREATER_EQUAL:
                return !index.columns().isEmpty() && 
                       index.columns().get(0).equals(column);
                       
            default:
                return false;
        }
    }

    private Expression extractIndexCondition(Expression predicate, IndexMetadata indexMetadata) {
        // For compound predicates, extract the part that can use the index
        if (predicate.getType() == Expression.ExpressionType.LOGICAL && 
            predicate.getOperator() == Expression.Operator.AND) {
            Expression leftCondition = extractIndexCondition(predicate.getLeft(), indexMetadata);
            if (leftCondition != null) {
                return leftCondition;
            }
            return extractIndexCondition(predicate.getRight(), indexMetadata);
        }
        
        // For simple predicates, check if they can use the index
        if (isIndexUsable(indexMetadata, predicate)) {
            return predicate;
        }
        
        return null;
    }

    private Expression extractRemainingPredicate(Expression original, Expression indexCondition) {
        if (original == null || indexCondition == null) {
            return original;
        }
        
        // If they're the same predicate, there's no remaining condition
        if (original.equals(indexCondition)) {
            return null;
        }
        
        // For AND predicates, remove the index condition
        if (original.getType() == Expression.ExpressionType.LOGICAL && 
            original.getOperator() == Expression.Operator.AND) {
            Expression left = original.getLeft();
            Expression right = original.getRight();
            
            if (left.equals(indexCondition)) {
                return right;
            } else if (right.equals(indexCondition)) {
                return left;
            } else {
                // Recursively check subtrees
                Expression remainingLeft = extractRemainingPredicate(left, indexCondition);
                Expression remainingRight = extractRemainingPredicate(right, indexCondition);
                
                if (remainingLeft == null) {
                    return remainingRight;
                } else if (remainingRight == null) {
                    return remainingLeft;
                } else {
                    return Expression.logical(Expression.Operator.AND, remainingLeft, remainingRight);
                }
            }
        }
        
        // For other types, keep the original
        return original;
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
            Expression predicate, 
            List<RangeTableEntry> rangeTable) {
        
        // Find the appropriate index for this predicate
        IndexMetadata indexMetadata = findBestIndex(metadata, predicate);
        if (indexMetadata == null) {
            throw new IllegalStateException("No suitable index found for predicate");
        }

        // Split predicate into index condition and filter condition
        Expression indexCondition = extractIndexCondition(predicate, indexMetadata);
        Expression filterPredicate = extractRemainingPredicate(predicate, indexCondition);

        // Find the RTE for this table
        RangeTableEntry rte = findRangeTableEntry(rangeTable, tableName);

        return new QueryTree(
            QueryOperator.INDEX_SCAN,
            new IndexScanOperation(rte, indexMetadata, indexCondition, filterPredicate),
            metadata.columnNames(),
            rangeTable
        );
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

    private QueryTree addFilter(QueryTree input, Expression predicate, QueryContext queryContext) {
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
    

    private Expression generatePredicate(ParseTree expr, QueryContext queryContext) {
        return ExpressionBuilder.build(expr, queryContext.getRangeTable());
    }

    private Object parseValue(ParseTree value) {
        return TypeConverter.convertValue(value);
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

    private QueryPredicate generateComparisonPredicate(ParseTree expr, QueryContext queryContext) {
        ParseTree left = expr.getChild(0);
        ParseTree operator = expr.getChild(1);
        ParseTree right = expr.getChild(2);

        return QueryPredicate.comparison(operator.getType(), left.getValue(), parseValue(right));
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
            Expression predicate = generatePredicate(whereClause.getChild(0), queryContext);
            result = optimizeAccessPath(result, predicate, queryContext);
        }

        return result;
    }
} 