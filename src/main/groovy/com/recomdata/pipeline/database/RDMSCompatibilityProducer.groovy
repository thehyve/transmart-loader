package com.recomdata.pipeline.database

import com.recomdata.pipeline.util.FileRef

import javax.enterprise.context.ApplicationScoped
import javax.enterprise.inject.Produces
import javax.inject.Inject

@ApplicationScoped
class RDMSCompatibilityProducer {

    @Inject @FileRef('Common') Properties common

    @Produces @ApplicationScoped RDMSCompatibility produceRDMSCompatibility(
            @RDMS('PostgreSQL') RDMSCompatibility postgresCompat) {

        switch (common.driver_class) {
            case 'org.postgresql.Driver':
                return postgresCompat
            case 'org.h2.Driver':
                def mode = 'H2'
                def matches
                if ((matches = common.url =~ /(?i);MODE=([^;]+)/)) {
                    mode = matches[0][1]
                }
                switch (mode) {
                    case 'PostgreSQL':
                        return postgresCompat
                    default:
                        throw new IllegalStateException("Unrecognized H2 mode: $mode")
                }
            default:
                throw new IllegalStateException("Unrecognized driver: ${common.driver_class}")
        }
    }
}
