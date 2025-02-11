package com.easydb.sql.ddl;

import com.easydb.index.IndexType;
import com.easydb.sql.parser.ParseTree;
import com.easydb.sql.parser.ParseTreeType;
import com.easydb.storage.metadata.IndexMetadata;

import java.util.*;

/**
 * Builds IndexMetadata from a CREATE INDEX parse tree.
 * Follows PostgreSQL's pg_index structure.
 */
public class IndexMetadataBuilder {
    
    public static IndexMetadata fromParseTree(ParseTree parseTree) {
        // Get index name
        ParseTree indexRef = findChildOfType(parseTree, ParseTreeType.INDEX_REF);
        if (indexRef == null) {
            throw new IllegalArgumentException("Missing index name in CREATE INDEX statement");
        }
        String indexName = indexRef.getValue();

        // Get table name
        ParseTree tableRef = findChildOfType(parseTree, ParseTreeType.TABLE_REF);
        if (tableRef == null) {
            throw new IllegalArgumentException("Missing table name in CREATE INDEX statement");
        }
        String tableName = tableRef.getValue();

        // Get column list
        ParseTree columnList = findChildOfType(parseTree, ParseTreeType.COLUMN_REF);
        if (columnList == null) {
            throw new IllegalArgumentException("Missing column list in CREATE INDEX statement");
        }

        // Process index columns
        List<String> columns = new ArrayList<>();
        for (ParseTree columnRef : columnList.getChildren()) {
            if (columnRef.getType() != ParseTreeType.COLUMN_REF) {
                throw new IllegalArgumentException("Invalid column reference in index definition");
            }
            columns.add(columnRef.getValue());
        }

        // Get index type (default to BTREE if not specified)
        IndexType indexType = IndexType.HASH;
        ParseTree typeNode = findChildOfType(parseTree, ParseTreeType.INDEX_REF);
        if (typeNode != null) {
            indexType = parseIndexType(typeNode.getValue());
        }

        // Get uniqueness constraint (default to false if not specified)
        boolean isUnique = false;
        ParseTree uniqueNode = findChildOfType(parseTree, ParseTreeType.UNIQUE_CONSTRAINT);
        if (uniqueNode != null) {
            isUnique = true;
        }

        return new IndexMetadata(
            indexName,
            tableName,
            columns,
            isUnique,
            indexType
        );
    }

    private static IndexType parseIndexType(String typeStr) {
        return Enum.valueOf(IndexType.class, typeStr);
    }

    private static ParseTree findChildOfType(ParseTree parent, ParseTreeType type) {
        for (ParseTree child : parent.getChildren()) {
            if (child.getType() == type) {
                return child;
            }
        }
        return null;
    }
}