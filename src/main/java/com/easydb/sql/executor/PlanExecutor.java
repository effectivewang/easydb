package com.easydb.sql.executor;

import com.easydb.storage.Tuple;
import java.util.Optional;

/**
 * Base interface for plan executors, following PostgreSQL's iterator model.
 */
public interface PlanExecutor {
    /**
     * Initialize the executor
     */
    void init();

    /**
     * Get next tuple, returns empty if no more tuples
     * Similar to PostgreSQL's ExecProcNode
     */
    Optional<Tuple> next();

    /**
     * Cleanup resources
     */
    void close();

    /**
     * Reset the executor to start from beginning
     */
    void rescan();
} 