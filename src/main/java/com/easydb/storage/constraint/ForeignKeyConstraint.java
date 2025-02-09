package com.easydb.storage.constraint;

import java.util.List;

public class ForeignKeyConstraint extends Constraint {
    private final String referenceTable;
    private final List<String> referenceColumns;
    private final FKAction onDelete;
    private final FKAction onUpdate;

    public ForeignKeyConstraint(String name, String tableName, List<String> columns,
                              String referenceTable, List<String> referenceColumns,
                              FKAction onDelete, FKAction onUpdate) {
        super(name, ConstraintType.FOREIGN_KEY, tableName, columns);
        this.referenceTable = referenceTable;
        this.referenceColumns = referenceColumns;
        this.onDelete = onDelete;
        this.onUpdate = onUpdate;
    }

    public String getReferenceTable() {
        return referenceTable;
    }

    public List<String> getReferenceColumns() {
        return referenceColumns;
    }

    public enum FKAction {
        NO_ACTION, CASCADE, SET_NULL, SET_DEFAULT, RESTRICT
    }
} 