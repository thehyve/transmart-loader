package org.transmartproject.pipeline.dictionary

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.transmartproject.pipeline.transmart.BioDataCorrelDescr
import org.transmartproject.pipeline.transmart.BioDataCorrelation
import org.transmartproject.pipeline.transmart.BioMarker
import org.transmartproject.pipeline.util.Util

class CorrelationLoader {
    private static final Logger log = Logger.getLogger(CorrelationLoader)

    Sql sqlBiomart
    Sql sqlDeapp

    BioDataCorrelation bioDataCorrelation
    long bioDataCorrelDescrId

    public CorrelationLoader(String correlation) {
        log.info("Start loading property file ...")
        Properties props = Util.loadConfiguration('')
        sqlBiomart = Util.createSqlFromPropertyFile(props, "biomart")
        sqlDeapp = Util.createSqlFromPropertyFile(props, "deapp")

        bioDataCorrelation = new BioDataCorrelation()
        bioDataCorrelation.setBiomart(sqlBiomart)

        retrieveBioDataCorrelId(correlation)
    }

    public void close() {
        sqlBiomart.close()
        sqlDeapp.close()
    }

    /** Retrieves the bioDataCorrelDescrID or inserts a new one
     *  if it doesn't exist.
     */
    public void retrieveBioDataCorrelId(correlation) {

        // Get data correlation description id
        BioDataCorrelDescr bioDataCorrelDescr = new BioDataCorrelDescr()
        bioDataCorrelDescr.setBiomart(sqlBiomart)
        bioDataCorrelDescrId = bioDataCorrelDescr.getBioDataCorrelId(correlation, correlation)
        if (bioDataCorrelDescrId == 0) {
            // Insert a new description
            bioDataCorrelDescr.insertBioDataCorrelDescr(correlation, "", correlation)
            // Get the id of the inserted entry
            bioDataCorrelDescrId = bioDataCorrelDescr.getBioDataCorrelId(correlation, correlation)
        }

    }

    public boolean insertCorrelation(CorrelationEntry correlationEntry) {

        // Look up the BIO_MARKER_ID for symbol 1
        BioMarker bioMarker = new BioMarker()
        bioMarker.setBiomart(sqlBiomart)
        Long bioMarkerId1 = bioMarker.getBioMarkerIDBySymbol(correlationEntry.symbol1,
                correlationEntry.organism, correlationEntry.markerType1)

        // Look up the BIO_MARKER_ID for symbol 2
        bioMarker = new BioMarker()
        bioMarker.setBiomart(sqlBiomart)
        Long bioMarkerId2 = bioMarker.getBioMarkerIDBySymbol(correlationEntry.symbol2,
                correlationEntry.organism, correlationEntry.markerType2)

        // Add the correlation to BIO_DATA_CORRELATION if possible
        if (bioMarkerId1 != null && bioMarkerId2 != null) {
            bioDataCorrelation.insertBioDataCorrelation(bioMarkerId1, bioMarkerId2, bioDataCorrelDescrId)
            return true;
        }
        return false;
    }
}
