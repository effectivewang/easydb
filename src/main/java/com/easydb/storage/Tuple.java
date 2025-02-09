package com.easydb.storage;

import java.util.List;

import com.easydb.storage.metadata.TableMetadata;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Represents a tuple (row) in a table.
 */
public class Tuple {
    private final TupleId id;
    private final byte[] values;
    private final List<Integer> valueLengths;

    public Tuple(TupleId id, List<Object> rowValues) {
        this.id = id;
        this.valueLengths = rowValues.stream()
            .map(value -> ByteUtils.getSerializedLength(value))
            .collect(Collectors.toList());

        this.values = ByteUtils.serializeValues(rowValues, valueLengths);
    }

    public TupleId id() {
        return id;
    }

    public List<Object> getValues(List<Class<?>> types) {
        return ByteUtils.deserializeValues(values, valueLengths, types);
    }

    public Object getValue(TableMetadata table, String columnName) {
        List<Object> values = getValues(table.columnTypes());
        int index = table.columnNames().indexOf(columnName);
        return values.get(index);
    }

    public Tuple withUpdatedValues(List<Object> newValues) {
        return new Tuple(id, newValues);
    }
} 
