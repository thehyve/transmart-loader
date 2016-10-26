package org.transmartproject.pipeline.dictionary


import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

/** Extracts gene to transcript correlation dictionary and loads it into the database.
 */
class GeneTransDictionary {
    private static final Logger log = Logger.getLogger(GeneTransDictionary)

    static main(args) {
        if (!args) {
            println "GeneTransDictionary <genetrans-dictionary.tsv>"
            System.exit(1)
        }

        def fileLocation = args[0]

        PropertyConfigurator.configure("conf/log4j.properties")
        File file = new File(fileLocation)
        GeneTransDictionary dict = new GeneTransDictionary()
        dict.loadData(file)
    }

    void loadData(File file) {
        if (!file.exists()) {
            log.error("File is not found: ${file.getAbsolutePath()}")
            return
        }

        CorrelationLoader correlationLoader = new CorrelationLoader("GENE TO TRANSCRIPT", "");

        try {
            file.eachLine(0) { line, number ->
                if (number == 0) {
                    return // Skip the header line
                }

                // Split values
                String[] split = line.split("\t")
                String transcriptId = split[0]
                String geneId = split[1]

                CorrelationEntry correlationEntry = new CorrelationEntry()
                correlationEntry.symbol1 = geneId
                correlationEntry.markerType1 = "GENE"
                correlationEntry.symbol2 = transcriptId
                correlationEntry.markerType2 = "TRANSCRIPT"
                correlationEntry.organism = "HOMO SAPIENS"
                correlationLoader.insertGeneTranscriptCorrelation(correlationEntry);
            }
        }
        finally {
            correlationLoader.close()
        }
    }
}
