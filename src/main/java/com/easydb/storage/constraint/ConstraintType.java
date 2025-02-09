package com.easydb.storage.constraint;

public enum ConstraintType {
    PRIMARY_KEY("p"),
    FOREIGN_KEY("f"),
    UNIQUE("u"),
    CHECK("c"),
    NOT_NULL("n");

    private final String code;
    ConstraintType(String code) { this.code = code; }
    public String getCode() { return code; }
}