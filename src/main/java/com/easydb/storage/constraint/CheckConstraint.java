package com.easydb.storage.constraint;

import java.util.*;

public class CheckConstraint extends Constraint {
    private final Map<String, Object> conditions;

    public CheckConstraint(String name, String tableName, List<String> columns,Map<String, Object> conditions) {
        super(name, ConstraintType.CHECK, tableName, columns);
        this.conditions = conditions;
    }

    public Map<String, Object> getConditions() {
        return this.conditions;
    }
} 