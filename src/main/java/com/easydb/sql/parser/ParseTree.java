package com.easydb.sql.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a node in the SQL parse tree.
 * Each node can have a type, value, and child nodes.
 */
public class ParseTree {
    private final ParseTreeType type;
    private final String value;
    private final List<ParseTree> children;

    public ParseTree(ParseTreeType type, String value) {
        this.type = type;
        this.value = value;
        this.children = new ArrayList<>();
    }

    public ParseTree(ParseTreeType type) {
        this(type, "");
    }

    public void addChild(ParseTree child) {
        children.add(child);
    }

    public ParseTreeType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public List<ParseTree> getChildren() {
        return new ArrayList<>(children);
    }

    public ParseTree getChild(int index) {
        return children.get(index);
    }

    public int getChildCount() {
        return children.size();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb, 0);
        return sb.toString();
    }

    private void toString(StringBuilder sb, int indent) {
        sb.append("  ".repeat(indent))
          .append(type)
          .append(value.isEmpty() ? "" : "(" + value + ")")
          .append("\n");
        
        for (ParseTree child : children) {
            child.toString(sb, indent + 1);
        }
    }
} 