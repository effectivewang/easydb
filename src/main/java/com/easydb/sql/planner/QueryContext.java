package com.easydb.sql.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryContext {
    private final List<RangeTableEntry> rangeTable;
    private final Map<String, RangeTableEntry> columnToRteMap;  // For column resolution

    public QueryContext(List<RangeTableEntry> rangeTable) {
        this.rangeTable = new ArrayList<>(rangeTable);
        this.columnToRteMap = new HashMap<>();
        buildColumnMap();
    }

    private void buildColumnMap() {
        for (RangeTableEntry rte : rangeTable) {
            for (String column : rte.getMetadata().columnNames()) {
                String qualifiedName = rte.getQualifiedName(column);
                columnToRteMap.put(qualifiedName, rte);
                // Also map unqualified names if unambiguous
                if (!columnToRteMap.containsKey(column)) {
                    columnToRteMap.put(column, rte);
                }
            }
        }
    }

    public RangeTableEntry resolveColumn(String columnRef) {
        RangeTableEntry rte = columnToRteMap.get(columnRef);
        if (rte == null) {
            throw new IllegalArgumentException("Column not found: " + columnRef);
        }
        return rte;
    }

    public List<RangeTableEntry> getRangeTable() {
        return new ArrayList<>(rangeTable);
    }
} 