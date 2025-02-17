package com.easydb.sql.executor;

import com.easydb.storage.Storage;
import com.easydb.storage.Tuple;
import com.easydb.storage.TupleHeader;
import com.easydb.sql.executor.QueryExecutorState;
import com.easydb.sql.executor.PlanExecutor;
import com.easydb.sql.planner.operation.InsertOperation;
import com.easydb.storage.TupleId;
import java.util.Optional;
import java.util.List;

/**
 * Executes INSERT operations, similar to PostgreSQL's nodeModifyTable.c for INSERT
 */
public class InsertExecutor implements PlanExecutor {
    private final InsertOperation operation;
    private final Storage storage;
    private final QueryExecutorState state;
    private int currentValueIndex;
    private List<List<Object>> values;

    public InsertExecutor(
            InsertOperation operation,
            Storage storage,
            QueryExecutorState state) {
        this.operation = operation;
        this.storage = storage;
        this.state = state;
        this.currentValueIndex = 0;
    }

    @Override
    public void init() {
        // Get values to insert
        this.values = operation.getValues();
        this.currentValueIndex = 0;
    }

    @Override
    public Optional<Tuple> next() {
        if (currentValueIndex >= values.size()) {
            return Optional.empty();
        }

        // Get next row to insert
        List<Object> rowValues = values.get(currentValueIndex++);
        TupleId tupleId = TupleId.create(operation.getRangeTableEntry().getTableName());
        TupleHeader header = new TupleHeader(
            tupleId,
            operation.getRangeTableEntry().getMetadata(),
            state.getCurrentTransaction().getXid(),
            0L
        );
        // Create and insert tuple within transaction
        Tuple tuple = new Tuple(
            tupleId,
            rowValues,
            header,
            state.getCurrentTransaction().getXid()
        );
        
        storage.insertTuple(
            tuple,
            state.getCurrentTransaction()
        );

        return Optional.of(tuple);
    }

    @Override
    public void close() {
        // No resources to clean up
    }

    @Override
    public void rescan() {
        currentValueIndex = 0;
    }
} 