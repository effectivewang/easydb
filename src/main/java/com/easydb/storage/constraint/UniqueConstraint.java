package com.easydb.storage.constraint;

import java.util.List;

public class UniqueConstraint extends Constraint {
    public UniqueConstraint(String name, String tableName, List<String> columns) {
        super(name, ConstraintType.UNIQUE, tableName, columns);
    }
}
