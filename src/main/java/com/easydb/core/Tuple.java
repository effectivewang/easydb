package com.easydb.core;

import java.util.List;
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

    public Tuple withUpdatedValues(List<Object> newValues) {
        return new Tuple(id, newValues);
    }
} 
