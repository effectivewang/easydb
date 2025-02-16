package com.easydb.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class for serializing and deserializing values to/from bytes.
 */
public class ByteUtils {
    private ByteUtils() {}

    public static byte[] serializeValues(List<Object> values, List<Integer> valueLengths) {
        int totalLength = valueLengths.stream().mapToInt(Integer::intValue).sum();
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value == null) {
                buffer.put(new byte[valueLengths.get(i)]);
            } else if (value instanceof Long) {
                buffer.putLong((Long) value);
            } else if (value instanceof Integer) {
                buffer.putInt((Integer) value);
            } else if (value instanceof Double) {
                buffer.putDouble((Double) value);
            } else if (value instanceof Boolean) {
                buffer.put((byte) ((Boolean) value ? 1 : 0));
            } else if (value instanceof String) {
                byte[] bytes = ((String) value).getBytes();
                buffer.put(bytes);
                buffer.put(new byte[valueLengths.get(i) - bytes.length]);
            }
        }

        return buffer.array();
    }

    public static List<Object> deserializeValues(byte[] bytes, List<Integer> valueLengths, List<Class<?>> types) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        List<Object> values = new ArrayList<>(types.size());

        for (int i = 0; i < types.size(); i++) {
            Class<?> type = types.get(i);
            int length = valueLengths.get(i);

            if (length == 0) {
                values.add(null);
                continue;
            }

            if (type.equals(Long.class)) {
                values.add(buffer.getLong());
            } else if (type.equals(Integer.class)) {
                values.add(buffer.getInt());
            } else if (type.equals(Double.class)) {
                values.add(buffer.getDouble());
            } else if (type.equals(Boolean.class)) {
                values.add(buffer.get() == 1);
            } else if (type.equals(String.class)) {
                byte[] stringBytes = new byte[length];
                buffer.get(stringBytes);
                values.add(new String(stringBytes).trim());
            } else {
                buffer.position(buffer.position() + length);
                values.add(null);
            }

        }

        return values;
    }

    public static int getSerializedLength(Object value) {
        if (value instanceof Long) {
            return Long.BYTES;
        } else if (value instanceof Integer) {
            return Integer.BYTES;
        } else if (value instanceof Double) {
            return Double.BYTES;
        } else if (value instanceof Boolean) {
            return 1;
        } else if (value instanceof String) {
            return ((String) value).getBytes().length;
        } else if (value == null) {
            return 0;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }
    }
} 