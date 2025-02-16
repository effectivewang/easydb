package com.easydb.sql.parser;

public class ParseTreeHelper {
    public static ParseTree getChildOfType(ParseTree tree, ParseTreeType type) {
        return tree.getChildren().stream()
            .filter(child -> child.getType() == type)
            .findFirst()
            .orElse(null);
    }
}
