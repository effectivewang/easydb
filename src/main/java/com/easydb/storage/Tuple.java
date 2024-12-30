package com.easydb.storage;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a tuple (row) in a table.
 */
public class Tuple {
    private final TupleId id;
    private final byte[] values;
    private final List<Integer> valueLengths;

    public Tuple(TupleId id, List<Object> values) {
        this.id = id;
        this.valueLengths = values.stream()
            .map(value -> ByteUtils.getSerializedLength(value))
            .collect(Collectors.toList());
        this.values = ByteUtils.serializeValues(values, valueLengths);
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
