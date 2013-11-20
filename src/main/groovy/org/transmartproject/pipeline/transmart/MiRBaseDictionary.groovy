package org.transmartproject.pipeline.transmart

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

    static main(args) {
        if(!args) {
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

    protected void insertMiRBase(miRBaseEntry){
        def geneSymbol = miRBaseEntry["entrezgene"]
        def description = miRBaseEntry["description"]
        def organism = miRBaseEntry["organism"]
        def source_code = miRBaseEntry["source"]
        def external_id = miRBaseEntry["extId"]
        def markerType = "MIRNA"


        log.info "Insert $organism:$geneSymbol:$external_id:$markerType into BIO_MARKER ..."


        String qry = """ insert into bio_marker(bio_marker_name, bio_marker_description, organism,
                                primary_source_code, primary_external_id, bio_marker_type)
                                values(?, ?, ?, ?, ?, ?) """
        if (isBioMarkerExist(external_id, markerType, organism)) {
            log.info "$organism:$geneSymbol:$external_id:$markerType already exists in BIO_MARKER ..."
        }
        else {

            log.info "Insert $organism:$geneSymbol:$external_id:$markerType into BIO_MARKER ..."
            sqlBiomart.executeInsert(qry, [
                    geneSymbol,
                    description,
                    organism,
                    source_code,
                    external_id,
                    markerType,
            ])

        }

    }

    protected boolean isBioMarkerExist(String geneId, String markerType, String organism){
        String qry = "select count(*) from bio_marker where primary_external_id=? and organism=? and bio_marker_type=?"
        def res = sqlBiomart.firstRow(qry, [geneId, organism, markerType])
        if(res[0] > 0) return true
        else return false
    }

}
