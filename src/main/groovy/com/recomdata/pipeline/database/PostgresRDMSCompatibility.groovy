package com.recomdata.pipeline.database

import groovy.sql.Sql
import org.slf4j.Logger

import javax.enterprise.context.ApplicationScoped
import javax.inject.Inject

@ApplicationScoped
@RDMS('PostgreSQL')
class PostgresRDMSCompatibility implements RDMSCompatibility {

    @Inject Logger log

    @Inject Sql sql

    @Override
    void createTemporaryTable(String name, String modelTable) {
        /* do not use LIKE because that will keep not null constraints,
         * even with EXCLUDING CONSTRAINTS.
         * Session scoped temp trables (ON COMMIT PRESERVE ROWS) are the
         * default, so we do not need to make it explicit
         */
        sql.execute """
                    CREATE TEMPORARY TABLE $name AS
                        SELECT * FROM $modelTable WHERE FALSE
                    """.toString() /* convert to string to avoid placeholders */

        log.debug 'Created temporary table {} from model {}', name, modelTable
    }

    @Override
    void analyzeTable(String name) {
        sql.execute """ANALYZE $name""".toString()

        log.debug 'Analyzed table {}', name
    }

    @Override
    String getComplementOperator() {
        'EXCEPT'
    }
}
