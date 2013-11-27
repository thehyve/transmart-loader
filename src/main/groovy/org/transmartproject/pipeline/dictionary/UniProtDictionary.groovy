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

                // Extract and insert biomarker (including search keywords and terms)
                BioMarkerEntry bioMarkerEntry = new BioMarkerEntry("PROTEIN", "Protein")
                String[] split = line.split(";")
                bioMarkerEntry.symbol = split[0]
                bioMarkerEntry.description = split[2]
                bioMarkerEntry.synonyms.add(split[1])
                bioMarkerEntry.synonyms.add(split[2])
                bioMarkerEntry.externalID = split[0]
                bioMarkerEntry.source = "UniProt"
                bioMarkerEntry.organism = "HOMO SAPIENS"
                //dictionaryLoader.insertBiomarker(bioMarkerEntry)

                // Extract and insert data correlation
                CorrelationEntry correlationEntry = new CorrelationEntry()
                correlationEntry.symbol1 = split[0]
                correlationEntry.markerType1 = "PROTEIN"
                correlationEntry.symbol2 = split[3]
                correlationEntry.markerType2 = "GENE"
                correlationEntry.organism = "HOMO SAPIENS"
                long correlationDescriptionID = 386137297; //TODO: where to get this?
                //correlationLoader.insertCorrelation(correlationEntry, correlationDescriptionID);

            }
        }
        finally {
            dictionaryLoader.close()
            correlationLoader.close()
        }
    }

}
