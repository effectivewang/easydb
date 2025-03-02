package com.easydb.sql.parser;

public class ParseTreeHelper {
    public static ParseTree getChildOfType(ParseTree tree, ParseTreeType type) {
        return tree.getChildren().stream()
            .filter(child -> child.getType() == type)
            .findFirst()
            .orElse(null);
    }

    public static Object getValue(ParseTree tree) {
        switch (tree.getType()) {
            case INTEGER_TYPE:
                return Integer.parseInt(tree.getValue());
            case DOUBLE_TYPE:
                return Double.parseDouble(tree.getValue());
            case BOOLEAN_TYPE:
                return Boolean.parseBoolean(tree.getValue());
            case STRING_TYPE:
                return tree.getValue();
            case NULL_TYPE:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported type: " + tree.getType());
        }
    }
}
