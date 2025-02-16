package com.easydb.sql.planner;

import com.easydb.storage.Storage;
import com.easydb.storage.metadata.TableMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;

public class ParseTreeAnalyzer {
    private final Storage storage;
    private int nextRteIndex = 1;

    public ParseTreeAnalyzer(Storage storage) {
        this.storage = storage;
    }

    public QueryContext analyze(ParseTree parseTree) {
        List<RangeTableEntry> rangeTable = new ArrayList<>();

        // Find FROM clause and build range table
        ParseTree fromClause = findChildOfType(parseTree, ParseTreeType.FROM_CLAUSE);
        if (fromClause != null) {
            buildRangeTable(fromClause, rangeTable);
        }

        return new QueryContext(rangeTable);
    }

    private ParseTree findChildOfType(ParseTree tree, ParseTreeType type) {
        for (ParseTree child : tree.getChildren()) {
            if (child.getType() == type) {
                return child;
            }
        }
        return null;
    }

    private void buildRangeTable(ParseTree fromClause, List<RangeTableEntry> rangeTable) {
        for (ParseTree tableRef : fromClause.getChildren()) {
            String tableName = tableRef.getValue();
            String alias = tableRef.getType() == ParseTreeType.ALIAS ? tableRef.getChild(0).getValue() : null;
            TableMetadata metadata = storage.getTableMetadata(tableName);
            
            RangeTableEntry rte = new RangeTableEntry(
                tableName,
                alias,
                metadata,
                nextRteIndex++
            );
            rangeTable.add(rte);
        }
    }
} 