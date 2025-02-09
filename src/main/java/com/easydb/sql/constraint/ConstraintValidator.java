package com.easydb.sql.constraint;

import com.easydb.storage.metadata.TableMetadata;
import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.sql.planner.QueryPredicate;
import java.util.List;
import java.util.Objects;

public class ConstraintValidator {
    private final Storage storage;

    public ConstraintValidator(Storage storage) {
        this.storage = storage;
    }

    public void validate(TableMetadata table, Tuple tuple) {
        for (Constraint constraint : table.constraints().values()) {
            switch (constraint.getType()) {
                case PRIMARY_KEY:
                    validatePrimaryKey((PrimaryKeyConstraint)constraint, tuple, table);
                    break;
                case FOREIGN_KEY:
                    validateForeignKey((ForeignKeyConstraint)constraint, tuple, table);
                    break;
                case UNIQUE:
                    validateUnique((UniqueConstraint)constraint, tuple, table);
                    break;
                case CHECK:
                    validateCheck((CheckConstraint)constraint, tuple, table);
                    break;
                case NOT_NULL:
                    validateNotNull((NotNullConstraint)constraint, tuple, table);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported constraint type: " + constraint.getType());
            }
        }
    }

    private void validatePrimaryKey(PrimaryKeyConstraint constraint, Tuple tuple, TableMetadata table) {
        // Check for NULL values
        for (String column : constraint.getColumns()) {
            if (tuple.getValue(table, column) == null) {
                throw new ConstraintViolationException(
                    "Primary key column '%s' cannot be null".formatted(column));
            }
        }

        // Check for uniqueness
        List<Object> pkValues = constraint.getColumns().stream()
            .map(column -> tuple.getValue(table, column))
            .toList();

        boolean exists = storage.findTuples(table.tableName())
            .stream()
            .anyMatch(existingTuple -> {
                List<Object> existingPkValues = constraint.getColumns().stream()
                    .map(column -> existingTuple.getValue(table, column))
                    .toList();
                return Objects.equals(pkValues, existingPkValues);
            });

        if (exists) {
            throw new ConstraintViolationException(
                "Duplicate key value violates primary key constraint '%s'"
                    .formatted(constraint.getName()));
        }
    }

    private void validateForeignKey(ForeignKeyConstraint constraint, Tuple tuple, TableMetadata table) {
        // Get referenced values from the current tuple
        List<Object> fkValues = constraint.getColumns().stream()
            .map(column -> tuple.getValue(table, column))
            .toList();

        // If all values are null, skip validation
        if (fkValues.stream().allMatch(Objects::isNull)) {
            return;
        }

        // Get referenced table metadata
        TableMetadata refTable = storage.getTableMetadata(constraint.getReferenceTable());
        if (refTable == null) {
            throw new ConstraintViolationException(
                "Referenced table '%s' does not exist"
                    .formatted(constraint.getReferenceTable()));
        }

        // Check if referenced values exist in the parent table
        boolean exists = storage.findTuples(constraint.getReferenceTable())
            .stream()
            .anyMatch(parentTuple -> {
                List<Object> parentValues = constraint.getReferenceColumns().stream()
                    .map(column -> parentTuple.getValue(refTable, column))
                    .toList();
                return Objects.equals(fkValues, parentValues);
            });

        if (!exists) {
            throw new ConstraintViolationException(
                "Foreign key violation: referenced values in '%s' do not exist in table '%s'"
                    .formatted(constraint.getName(), constraint.getReferenceTable()));
        }
    }

    private void validateUnique(UniqueConstraint constraint, Tuple tuple, TableMetadata table) {
        List<Object> uniqueValues = constraint.getColumns().stream()
            .map(column -> tuple.getValue(table, column))
            .toList();

        boolean exists = storage.findTuples(table.tableName())
            .stream()
            .anyMatch(existingTuple -> {
                List<Object> existingValues = constraint.getColumns().stream()
                    .map(column -> existingTuple.getValue(table, column))
                    .toList();
                return Objects.equals(uniqueValues, existingValues);
            });

        if (exists) {
            throw new ConstraintViolationException(
                "Duplicate key value violates unique constraint '%s'"
                    .formatted(constraint.getName()));
        }
    }

    private void validateCheck(CheckConstraint constraint, Tuple tuple, TableMetadata table) {
        QueryPredicate predicate = constraint.getPredicate();
        if (!evaluatePredicate(predicate, tuple, table)) {
            throw new ConstraintViolationException(
                "Check constraint '%s' violated".formatted(constraint.getName()));
        }
    }

    private void validateNotNull(NotNullConstraint constraint, Tuple tuple, TableMetadata table) {
        for (String column : constraint.getColumns()) {
            if (tuple.getValue(table, column) == null) {
                throw new ConstraintViolationException(
                    "Column '%s' cannot be null".formatted(column));
            }
        }
    }

    private boolean evaluatePredicate(QueryPredicate predicate, Tuple tuple, TableMetadata table) {
        // Implementation for evaluating check constraint predicates
        // This will be implemented when we add support for check constraints
        throw new UnsupportedOperationException("Check constraint evaluation not implemented");
    }
} 