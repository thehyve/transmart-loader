package org.transmartproject.pipeline.dictionary

import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

/** Extracts miRNA data from file miRNA.dat (http://mirbase.org/ftp.shtml) and
 *  adds it to BIOMART.BIO_MARKER.
 */
class MiRBaseDictionary {
    private static final Logger log = Logger.getLogger(MiRBaseDictionary)

    static main(args) {
        if (!args) {
            println "MiRBaseDictionary <miRNA.dat>"
            System.exit(1)
        }

        def miRNAFileLocation = args[0]

        PropertyConfigurator.configure("conf/log4j.properties");
        File miRNAFile = new File(miRNAFileLocation)
        MiRBaseDictionary dict = new MiRBaseDictionary()
        dict.loadData(miRNAFile)
    }

    void loadData(File miRNAFile) {
        if (!miRNAFile.exists()) {
            log.error("File is not found: ${miRNAFile.getAbsolutePath()}")
            return
        }

        DictionaryLoader dictionaryLoader = new DictionaryLoader();

        try {
            BioMarkerEntry miRBaseEntry = new BioMarkerEntry("MIRNA", "miRNA")

            miRNAFile.eachLine {
                if (it.startsWith("//")) {
                    // Insert the current instance and start a new one
                    if (miRBaseEntry.organism && miRBaseEntry.symbol) {
                        dictionaryLoader.insertEntry(miRBaseEntry)
                    }
                    miRBaseEntry = new BioMarkerEntry("MIRNA", "miRNA")
                } else if (it.startsWith("ID")) {
                    // The id symbol (e.g. hsa-mir-100) will be a synonym and the external ID
                    String[] split = it.substring(5).split(" ")
                    String idSymbol = split[0]
                    miRBaseEntry.externalID = idSymbol
                    miRBaseEntry.synonyms.add(idSymbol)

                    // Extract the organism
                    if (it.contains("; MMU; ")) {
                        miRBaseEntry.organism = "MUS MUSCULUS"
                    }
                    if (it.contains("; RNO; ")) {
                        miRBaseEntry.organism = "RATTUS NORVEGICUS"
                    }
                    if (it.contains("; HSA; ")) {
                        miRBaseEntry.organism = "HOMO SAPIENS"
                    }
                } else if (it.startsWith("DE")) {
                    miRBaseEntry.description = it.substring(5)
                } else if (it.startsWith("DR")) {
                    if (it.contains("ENTREZGENE")) {
                        // Extract the Entrez gene code and use it as the symbol
                        // (There also seems to be an entrez gene id (str[1]),
                        // but we'll ignore it for now...)
                        String[] str = it.split("; ")
                        String code = str[-1]
                        if (code.endsWith('.')) {
                            code = code[0..-2]
                        }
                        miRBaseEntry.symbol = code.toUpperCase()
                        miRBaseEntry.source = 'Entrez'
                    }
                }
            }
        } finally {
            dictionaryLoader.close()
        }
    }

}
