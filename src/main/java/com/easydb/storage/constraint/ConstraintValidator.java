package com.easydb.storage.constraint;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.Tuple;
import com.easydb.storage.Storage;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Map;

public class ConstraintValidator {
    private final Storage storage;

    public ConstraintValidator(Storage storage) {
        this.storage = storage;
    }

    public void validate(TableMetadata table, Tuple tuple) {
        List<Object> values = tuple.getValues(table.columnTypes());;
        for (Constraint constraint : table.constraints()) {
            switch (constraint.getType()) {
                case PRIMARY_KEY:
                    validatePrimaryKey(constraint, values, table);
                    break;
                case FOREIGN_KEY:
                    validateForeignKey((ForeignKeyConstraint)constraint, values, table);
                    break;
                case UNIQUE:
                    validateUnique(constraint, values, table);
                    break;
                case CHECK:
                    validateCheck((CheckConstraint)constraint, tuple, table);
                    break;
                case NOT_NULL:
                    validateNotNull(constraint, tuple, table);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported constraint type: " + constraint.getType());
            }
        }
    }

    private Object getValue(List<Object> values, List<String> columList, String column) {
        int index = columList.indexOf(column);
        return values.get(index);
    }

    private void validatePrimaryKey(Constraint constraint, List<Object> values, TableMetadata metadata) {
        List<String> columnList = metadata.columnNames();
        Map<String, Object> condition = new HashMap<>();
        // Check for NULL values
        for (String column : constraint.getColumns()) {
            Object value = getValue(values, columnList, column);
            if (value == null) {
                throw new ConstraintViolationException(
                    "Primary key column '%s' cannot be null".formatted(column));
            }

            condition.put(null, value);
        }

        // Check for uniqueness
        List<Object> pkValues = constraint.getColumns().stream()
            .map(column -> getValue(values, columnList, column))
            .toList();

        boolean exists = storage.scanTuples(constraint.getTableName(), condition)
            .stream()
            .anyMatch(existingTuple -> {
                List<Object> existingPkValues = constraint.getColumns().stream()
                .map(column -> {
                    List<Object> existingValues = existingTuple.getValues(metadata.columnTypes());
                    return getValue(existingValues, columnList, column);
                }).toList();
                return Objects.equals(pkValues, existingPkValues);
            });

        if (exists) {
            throw new ConstraintViolationException(
                "Duplicate key value violates primary key constraint '%s'"
                    .formatted(constraint.getName()));
        }
    }

    private void validateForeignKey(ForeignKeyConstraint constraint, List<Object> values, TableMetadata metadata) {
        List<String> columnList = metadata.columnNames();
        Map<String, Object> condition = new HashMap<>();

        // Get referenced values from the current tuple
        List<Object> fkValues = constraint.getColumns().stream()
            .map(column -> {
                Object value = getValue(values, columnList, column);
                condition.put(column, value);
                return value;
            }).toList();

        // If all values are null and the constraint allows it, skip validation
        if (fkValues.stream().allMatch(Objects::isNull)) {
            return;
        }

        // Check if referenced values exist in the parent table
        boolean exists = storage.scanTuples(constraint.getReferenceTable(), condition)
            .stream()
            .anyMatch(parentTuple -> {
                List<Object> parentValues = constraint.getReferenceColumns().stream()
                .map(column -> {
                    List<Object> existingValues = parentTuple.getValues(metadata.columnTypes());
                    return getValue(existingValues, columnList, column);
                }).toList();
                return Objects.equals(fkValues, parentValues);
            });

        if (!exists) {
            throw new ConstraintViolationException(
                "Foreign key violation: referenced values in '%s' do not exist in table '%s'"
                    .formatted(constraint.getName(), constraint.getReferenceTable()));
        }
    }

    private void validateUnique(Constraint constraint, List<Object> values, TableMetadata metadata) {
        List<String> columnList = metadata.columnNames();
        Map<String, Object> condition = new HashMap<>();
        List<Object> uniqueValues = constraint.getColumns().stream()
            .map(column -> {
                Object value = getValue(values, metadata.columnNames(), column);
                condition.put(column, value);
                return value;
            }).toList();

        boolean exists = storage.scanTuples(constraint.getTableName(), condition)
            .stream()
            .anyMatch(existingTuple -> {
                List<Object> existingValues = constraint.getColumns().stream()
                    .map(column -> {
                        List<Object> temp = existingTuple.getValues(metadata.columnTypes());
                        return getValue(temp, metadata.columnNames(), column);
                    }).toList();
                return Objects.equals(uniqueValues, existingValues);
            });

        if (exists) {
            throw new ConstraintViolationException(
                "Duplicate key value violates unique constraint '%s'"
                    .formatted(constraint.getName()));
        }
    }

    private void validateCheck(CheckConstraint constraint, Tuple tuple, TableMetadata table) {
        if (!evaluatePredicate(constraint.getConditions(), tuple)) {
            throw new ConstraintViolationException(
                "Check constraint '%s' violated".formatted(constraint.getName()));
        }
    }

    private void validateNotNull(Constraint constraint, Tuple tuple, TableMetadata table) {
        for (String column : constraint.getColumns()) {
            if (tuple.getValue(table, column) == null) {
                throw new ConstraintViolationException(
                    "Column '%s' cannot be null".formatted(column));
            }
        }
    }

    private boolean evaluatePredicate(Map<String, Object> conditions, Tuple tuple) {
        // Implementation for evaluating check constraint predicates
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
