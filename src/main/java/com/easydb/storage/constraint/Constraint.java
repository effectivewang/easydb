package com.easydb.storage.constraint;

import java.util.List;

/**
 * Represents table constraints following PostgreSQL's pg_constraint structure.
 */
public abstract class Constraint {
    private final String name;
    private final ConstraintType type;
    private final String tableName;
    private final List<String> columns;

    protected Constraint(String name, ConstraintType type, String tableName, List<String> columns) {
        this.name = name;
        this.type = type;
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getName() { return name; }
    public ConstraintType getType() { return type; }
    public String getTableName() { return tableName; }
    public List<String> getColumns() { return columns; }
} 