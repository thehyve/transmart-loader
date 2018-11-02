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

    public void insertEntry(BioMarkerEntry bmEntry) {

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
        // Check if it exists and insert into SEARCH_KEYWORD if not
        searchKeyword.insertSearchKeyword(bmEntry.symbol, bioMarkerID,
                bmEntry.externalID, bmEntry.source, bmEntry.markerType, bmEntry.displayCategory)
        // Determine the id of the keyword that was just inserted
        long searchKeywordID = searchKeyword.getSearchKeywordId(bmEntry.symbol,
                bmEntry.markerType, bioMarkerID)
        if (searchKeywordID > 0) {
            // SEARCH_KEYWORD_TERM (for symbol)
            insertTermIntoSearchKeywordTerm(bmEntry.symbol, searchKeywordID)

            // Insert synonyms (BIO_DATA_EXT_CODE and SEARCH_KEYWORD_TERM),
            // linking them to the corresponding biomarker and search keyword
            insertSynonyms(bmEntry, bioMarkerID, searchKeywordID)
        }
    }

    private void insertSynonyms(BioMarkerEntry bmEntry, long bioMarkerID, long searchKeywordID) {

        // Insert all synonyms from the BioMarkerEntry
        for (String synonym : bmEntry.synonyms) {
            insertSynonymIntoBioDataExtCode(synonym, bioMarkerID, bmEntry.markerType)
            insertTermIntoSearchKeywordTerm(synonym, searchKeywordID)
        }
    }

    private void insertSynonymIntoBioDataExtCode(String synonym, long bioMarkerID, String dataCategory) {

        // Insert into BIO_DATA_EXT_CODE
        BioDataExtCode bioDataExtCode = new BioDataExtCode()
        bioDataExtCode.setBiomart(sqlBiomart)
        if (bioDataExtCode.isBioDataExtCodeExist(bioMarkerID, synonym)) {
            log.info("$bioMarkerID:$synonym already exists in BIO_DATA_EXT_CODE")
        } else {
            log.info "Insert $bioMarkerID:$synonym:SYNONYM:BIO_MARKER.$dataCategory:Alias into BIO_DATA_EXT_CODE ..."
            bioDataExtCode.insertBioDataExtCode(bioMarkerID, synonym, dataCategory)
        }

    }

    private void insertTermIntoSearchKeywordTerm(String keywordTerm, long searchKeywordID) {

        // Insert into SEARCH_KEYWORD_TERM
        SearchKeywordTerm searchKeywordTerm = new SearchKeywordTerm()
        searchKeywordTerm.setSearchapp(sqlSearchApp)
        // check if exists and inserts if not:
        searchKeywordTerm.insertSearchKeywordTerm(keywordTerm, searchKeywordID, 2)

    }

}
