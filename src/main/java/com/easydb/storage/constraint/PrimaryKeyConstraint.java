package com.easydb.storage.constraint;

import java.util.List;

public class PrimaryKeyConstraint extends Constraint {
    public PrimaryKeyConstraint(String name, String tableName, List<String> columns) {
        super(name, ConstraintType.PRIMARY_KEY, tableName, columns);
    }
} 