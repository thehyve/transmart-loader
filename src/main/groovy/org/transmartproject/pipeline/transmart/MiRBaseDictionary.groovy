package org.transmartproject.pipeline.dictionary

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import org.transmartproject.pipeline.util.Util

/** Extracts miRNA data from file miRNA.dat (http://mirbase.org/ftp.shtml) and
 *  adds it to BIOMART.BIO_MARKER.
 */
class MiRBaseDictionary {
    private static final Logger log = Logger.getLogger(MiRBaseDictionary)

    Sql sqlBiomart
    Sql sqlSearchApp

    static main(args) {
        if (!args) {
            println "MiRBaseDictionary <miRNA.dat>"
            System.exit(1)
        }

        def miRNAFileLocation = args[0]

        PropertyConfigurator.configure("conf/log4j.properties");
        println args
        File miRNAFile = new File(miRNAFileLocation)
        MiRBaseDictionary dict = new MiRBaseDictionary()
        dict.loadData(miRNAFile)
    }

    void loadData(File miRNAFile) {
        if (!miRNAFile.exists()) {
            log.error("File is not found: ${miRNAFile.getAbsolutePath()}")
            return
        }

        log.info("Start loading property file ...")
        Properties props = Util.loadConfiguration('')
        sqlBiomart = Util.createSqlFromPropertyFile(props, "biomart")
        sqlSearchApp = Util.createSqlFromPropertyFile(props, "searchapp")
        try {
            def miRBaseEntry = [:]

            miRNAFile.eachLine {
                if (it.startsWith("//")) {
                    // Insert the current instance and start a new one
                    if (miRBaseEntry["organism"] && miRBaseEntry["entrezgene"]) {
                        insertMiRBase(miRBaseEntry)
                    }
                    miRBaseEntry = [:]
                } else if (it.startsWith("ID")) {
                    String[] str = it.substring(5).split(" ")
                    miRBaseEntry["id"] = str[0]
                    if (it.contains("; MMU; ")) {
                        miRBaseEntry["organism"] = "MUS MUSCULUS"
                    }
                    if (it.contains("; RNO; ")) {
                        miRBaseEntry["organism"] = "RATTUS NORVEGICUS"
                    }
                    if (it.contains("; HSA; ")) {
                        miRBaseEntry["organism"] = "HOMO SAPIENS"
                    }
                } else if (it.startsWith("DE")) {
                    String str = it.substring(5)
                    miRBaseEntry["description"] = str
                } else if (it.startsWith("DR")) {
                    if (it.contains("ENTREZGENE")) {
                        String[] str = it.split("; ")
                        String code = str[-1]
                        if (code.endsWith('.')) {
                            code = code[0..-2]
                        }
                        miRBaseEntry["entrezgene"] = code.toUpperCase()
                        miRBaseEntry["source"] = 'Entrez'
                        miRBaseEntry["extId"] = str[1]
                    }
                }
            }
        } finally {
            sqlBiomart.close()
        }
    }

    protected void insertMiRBase(miRBaseEntry) {
        def symbol = miRBaseEntry["entrezgene"]
        def description = miRBaseEntry["description"]
        def organism = miRBaseEntry["organism"]
        def source_code = miRBaseEntry["source"]
        def external_id = miRBaseEntry["id"]
        def markerType = "MIRNA"
        String synonym = miRBaseEntry["id"];


        if (isBioMarkerExist(external_id, markerType, organism)) {
            log.info "$organism:$symbol:$external_id:$markerType already exists in BIO_MARKER ..."

            // Retrieve its id
            String qry = "select bio_marker_id from bio_marker where primary_external_id=? and organism=? and bio_marker_type=?"
            GroovyRowResult rowResult = sqlBiomart.firstRow(qry, [external_id, organism, markerType])
            String bioMarkerID = rowResult[0]

            // Insert synonyms
            insertSynonyms(bioMarkerID, synonym, symbol);

        } else {

            // Insert into BIO_MARKER
            log.info "Insert $organism:$symbol:$external_id:$markerType into BIO_MARKER ..."
            String qry = """ insert into bio_marker(bio_marker_name, bio_marker_description, organism,
                                primary_source_code, primary_external_id, bio_marker_type)
                                values(?, ?, ?, ?, ?, ?) """
            List<List<Object>> ids = sqlBiomart.executeInsert(qry, [
                    symbol,
                    description,
                    organism,
                    source_code,
                    external_id,
                    markerType,
            ])

            // Retrieve the last SEQ_BIO_DATA_ID, which is the BIO_MARKER_ID used for
            // the inserted row in BIO_MARKER
            GroovyRowResult rowResult = sqlBiomart.firstRow(""" SELECT SEQ_BIO_DATA_ID.CURRVAL FROM DUAL """)
            Object bioMarkerID = rowResult.getProperty("CURRVAL")

            // Insert synonyms
            insertSynonyms(bioMarkerID, synonym, symbol);

        }

    }

    private void insertSynonyms(bioMarkerID, synonym, symbol) {
        String qry = "select count(*) from BIO_DATA_EXT_CODE where BIO_DATA_ID=? and CODE=?"
        GroovyRowResult res = sqlBiomart.firstRow(qry, [bioMarkerID, synonym])
        int count = res[0]

        if (count == 0) {
            log.info "Insert $bioMarkerID:$synonym:SYNONYM:BIO_MARKER.MIRNA:Alias into BIO_DATA_EXT_CODE ..."
            sqlBiomart.executeInsert("""
                insert into BIO_DATA_EXT_CODE(BIO_DATA_ID, CODE, CODE_TYPE, BIO_DATA_TYPE, CODE_SOURCE)
                  values(:bio_data_id, :code, :code_type, :bio_data_type, :code_source)
                """,
                    [bio_data_id: bioMarkerID,
                            code: synonym,
                            code_type: 'SYNONYM',
                            bio_data_type: 'BIO_MARKER.MIRNA',
                            code_source: 'Alias'])
        } else {
            log.info("$bioMarkerID:$synonym already exists in BIO_DATA_EXT_CODE");
        }
    }

    protected boolean isBioMarkerExist(String geneId, String markerType, String organism) {
        String qry = "select count(*) from bio_marker where primary_external_id=? and organism=? and bio_marker_type=?"
        GroovyRowResult res = sqlBiomart.firstRow(qry, [geneId, organism, markerType])
        int count = res[0]
        return (count > 0)
    }

}
