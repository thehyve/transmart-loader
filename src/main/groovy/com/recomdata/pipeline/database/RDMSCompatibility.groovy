package com.recomdata.pipeline.database

/**
 * Interface for abstracting some database differences.
 */
public interface RDMSCompatibility {

    /**
     * Creates a session temporary table with the same structure of an existing
     * table.
     *
     * @param name       the name of the temporary table. Possibly quoted.
     * @param modelTable the name of the model table. Possibly schema-qualified
     *                   and quoted.
     */
    void createTemporaryTable(String name, String modelTable)

    /**
     * Analyzes a tables, collecting statistics relevant to query planning.
     * May be implemented as no-op, as this is mostly a performance
     * requirement for PostgreSQL
     *
     * @param name the possibly quoted or schema-qualified table name
     */
    void analyzeTable(String name)

    /**
     * The operator that computes the relative complement of two sets, like
     * MINUS in Oracle and EXCEPT in PostgreSQL.
     *
     * @return the relative complement operator
     */
    String getComplementOperator()
}
