package org.transmartproject.pipeline.dictionary


import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

/** Extracts protein dictionary and loads it into the database.
 */
class UniProtDictionary {
    private static final Logger log = Logger.getLogger(MiRBaseDictionary)

    static main(args) {
        if (!args) {
            println "UniProtDictionary <RBMannotation.csv>"
            System.exit(1)
        }

        def fileLocation = args[0]

        PropertyConfigurator.configure("conf/log4j.properties")
        File file = new File(fileLocation)
        UniProtDictionary dict = new UniProtDictionary()
        dict.loadData(file)
    }

    void loadData(File file) {
        if (!file.exists()) {
            log.error("File is not found: ${file.getAbsolutePath()}")
            return
        }

        DictionaryLoader dictionaryLoader = new DictionaryLoader();
        CorrelationLoader correlationLoader = new CorrelationLoader("PROTEIN TO GENE");

        try {
            file.eachLine(0) { line, number ->
                if (number == 0) {
                    return // Skip the header line
                }

                // Extract data
                String[] split = line.split(";")
                String uniProtNumber = split[0]
                String entryName = split[1] // symbol
                String proteinName = split[2]
                String geneSymbol = split[3]

                // Insert biomarker (including search keywords and terms)
                BioMarkerEntry bioMarkerEntry = new BioMarkerEntry("PROTEIN", "Protein")
                bioMarkerEntry.symbol = entryName
                bioMarkerEntry.description = proteinName
                bioMarkerEntry.synonyms.add(uniProtNumber)
                bioMarkerEntry.synonyms.add(proteinName)
                bioMarkerEntry.externalID = uniProtNumber
                bioMarkerEntry.source = "UniProt"
                bioMarkerEntry.organism = "HOMO SAPIENS"
                dictionaryLoader.insertEntry(bioMarkerEntry)

                // Insert data correlation
                CorrelationEntry correlationEntry = new CorrelationEntry()
                correlationEntry.symbol1 = entryName
                correlationEntry.markerType1 = "PROTEIN"
                correlationEntry.symbol2 = geneSymbol
                correlationEntry.markerType2 = "GENE"
                correlationEntry.organism = "HOMO SAPIENS"
                correlationLoader.insertCorrelation(correlationEntry);

            }
        }
        finally {
            dictionaryLoader.close()
            correlationLoader.close()
        }
    }

}
