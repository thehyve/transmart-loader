package org.transmartproject.pipeline.dictionary

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.transmartproject.pipeline.transmart.BioDataExtCode
import org.transmartproject.pipeline.transmart.BioMarker
import org.transmartproject.pipeline.transmart.SearchKeyword
import org.transmartproject.pipeline.transmart.SearchKeywordTerm
import org.transmartproject.pipeline.util.Util

/** Contains functionality to load symbols and synonyms into
 *  BIO_MARKER, BIO_DATA_EXT_CODE,
 *  SEARCH_KEYWORD, SEARCH_KEYWORD_TERM
 */
class DictionaryLoader {
    private static final Logger log = Logger.getLogger(DictionaryLoader)

    Sql sqlBiomart
    Sql sqlSearchApp

    public DictionaryLoader() {
        log.info("Start loading property file ...")
        Properties props = Util.loadConfiguration('')
        sqlBiomart = Util.createSqlFromPropertyFile(props, "biomart")
        sqlSearchApp = Util.createSqlFromPropertyFile(props, "searchapp")
    }

    public void close() {
        sqlBiomart.close()
        sqlSearchApp.close()
    }

    public void insertBiomarker(BioMarkerEntry bmEntry) {

        // BIO_MARKER
        BioMarker bioMarker = new BioMarker()
        bioMarker.setOrganism(bmEntry.organism)
        bioMarker.setBiomart(sqlBiomart)
        if (bioMarker.isBioMarkerExist(bmEntry.externalID, bmEntry.markerType)) {
            log.info "$bmEntry.organism:$bmEntry.symbol:$bmEntry.externalID:$bmEntry.markerType already exists in BIO_MARKER ..."
        } else {
            // Insert into BIO_MARKER
            bioMarker.insertBioMarker(bmEntry.symbol,
                    bmEntry.description,
                    bmEntry.externalID,
                    bmEntry.source,
                    bmEntry.markerType)
        }
        // Determine the id of the existing/inserted biomarker
        long bioMarkerID = bioMarker.getBioMarkerID(bmEntry.externalID,
                bmEntry.organism, bmEntry.markerType)


        // SEARCH_KEYWORD
        SearchKeyword searchKeyword = new SearchKeyword()
        searchKeyword.setSearchapp(sqlSearchApp)
        if (searchKeyword.isSearchKeywordExist(bmEntry.symbol, bmEntry.markerType, bioMarkerID)) {
            log.info "$bmEntry.symbol:$bmEntry.markerType:$bioMarkerID already exists in SEARCH_KEYWORD ..."
        } else {
            // Insert into SEARCH_KEYWORD
            searchKeyword.insertSearchKeyword(bmEntry.symbol, bioMarkerID,
                    bmEntry.externalID, bmEntry.source, bmEntry.markerType, bmEntry.displayCategory)
        }
        // Determine the id of the keyword that was just inserted
        long searchKeywordID = searchKeyword.getSearchKeywordId(bmEntry.symbol,
                bmEntry.markerType, bioMarkerID)


        // Insert synonyms (BIO_DATA_EXT_CODE and SEARCH_KEYWORD_TERM)
        insertSynonyms(bmEntry, bioMarkerID, searchKeywordID);
    }

    private void insertSynonyms(BioMarkerEntry bmEntry, long bioMarkerID, long searchKeywordID) {

        // Insert all synonyms from the BioMarkerEntry
        for (String synonym : bmEntry.synonyms) {
            insertSynonyms(synonym, bioMarkerID, searchKeywordID);
        }
    }

    private void insertSynonyms(String synonym, long bioMarkerID, long searchKeywordID) {

        // Insert into BIO_DATA_EXT_CODE
        BioDataExtCode bioDataExtCode = new BioDataExtCode()
        bioDataExtCode.setBiomart(sqlBiomart)
        if (bioDataExtCode.isBioDataExtCodeExist(bioMarkerID, synonym)) {
            log.info("$bioMarkerID:$synonym already exists in BIO_DATA_EXT_CODE")
        } else {
            log.info "Insert $bioMarkerID:$synonym:SYNONYM:BIO_MARKER.MIRNA:Alias into BIO_DATA_EXT_CODE ..."
            bioDataExtCode.insertBioDataExtCode(bioMarkerID, synonym, 'MIRNA')
        }

        // Insert into SEARCH_KEYWORD_TERM
        SearchKeywordTerm searchKeywordTerm = new SearchKeywordTerm()
        searchKeywordTerm.setSearchapp(sqlSearchApp)
        // check if exists and inserts if not:
        searchKeywordTerm.insertSearchKeywordTerm(synonym, searchKeywordID, 2)

    }

}
