package com.easydb.sql.planner.operation;

import com.easydb.sql.planner.QueryOperator;
import com.easydb.sql.planner.RangeTableEntry;
import com.easydb.sql.planner.QueryPredicate;
import com.easydb.storage.metadata.IndexMetadata;

/**
 * Represents an index scan operation, similar to PostgreSQL's IndexScan node.
 */
public class IndexScanOperation extends ScanOperation {
    private final IndexMetadata indexMetadata;
    private final QueryPredicate indexCondition;  // Condition used for index lookup

    public IndexScanOperation(
            RangeTableEntry rte, 
            IndexMetadata indexMetadata,
            QueryPredicate indexCondition,
            QueryPredicate filterPredicate) {  // Additional filter after index lookup
        super(rte, filterPredicate);
        this.indexMetadata = indexMetadata;
        this.indexCondition = indexCondition;
    }

    public IndexMetadata getIndexMetadata() {
        return indexMetadata;
    }

    public QueryPredicate getIndexCondition() {
        return indexCondition;
    }

    @Override
    public QueryOperator getOperator() {
        return QueryOperator.INDEX_SCAN;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder()
            .append("IndexScan using ")
            .append(indexMetadata.indexName())
            .append(" on ")
            .append(getRangeTableEntry().getDisplayName());
        
        if (indexCondition != null) {
            sb.append(" Index Cond: ").append(indexCondition);
        }
        if (getPredicate() != null) {
            sb.append(" Filter: ").append(getPredicate());
        }
        return sb.toString();
    }
} 