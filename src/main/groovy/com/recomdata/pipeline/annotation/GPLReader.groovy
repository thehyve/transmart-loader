/*************************************************************************
 * tranSMART - translational medicine data mart
 * 
 * Copyright 2008-2012 Janssen Research & Development, LLC.
 * 
 * This product includes software developed at Janssen Research & Development, LLC.
 * 
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License 
 * as published by the Free Software  * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *
 ******************************************************************/
  

package com.recomdata.pipeline.annotation

import com.recomdata.pipeline.util.Step
import com.recomdata.pipeline.util.StepExecution
import com.recomdata.pipeline.util.Util
import groovy.sql.Sql
import org.apache.log4j.PropertyConfigurator
import org.slf4j.Logger

import javax.enterprise.event.Observes
import javax.inject.Inject

class GPLReader {

	@Inject Logger log
	
	File  sourceDirectory
    Map expectedProbes

	@Inject Sql sql

	File probeInfo,
            snpGeneMap,
            snpMap

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		GPLReader al = new GPLReader()

		Properties props = Util.loadConfiguration("conf/loader.properties")

		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")
		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")

		al.loadGxGPL(props, biomart)
	}

	
	void loadGxGPL(Properties props, Sql biomart){

		if(props.get("skip_gx_gpl_loader").toString().toLowerCase().equals("yes")){
			log.info "Skip loading GX GPL annotation file(s) ..."
		}else{


			GexGPL gpl = new GexGPL()
			gpl.setSql(biomart)
			gpl.setAnnotationTable(props.get("annotation_table"))

			if(props.get("recreate_annotation_table").toString().toLowerCase().equals("yes")){
				log.info "Start recreating annotation table ${props.get("annotation_table")} for GPL GX annotation file(s) ..."
				gpl.createAnnotationTable()
			}


			String annotationSourceDirectory = props.get("annotation_source")
			String [] gplList = props.get("gpl_list").split(/\,/)
			gplList.each {
				File annotationSource = new File(annotationSourceDirectory + File.separator + "GPL." + it + ".txt")
				gpl.loadGxGPLs(annotationSource)
			}
		}
	}

	void processGPLs(@Observes @Step('processGPLs') StepExecution stepParams) {

        String inputFileName = stepParams['inputFileName'] /* actual can be several files... */
        ['probeInfo', 'snpMap', 'snpGeneMap', 'sourceDirectory', 'expectedProbes'].
                each { this."$it" = stepParams[it] } /* copy params to properties */

        String[] files = inputFileName.split(";")
        boolean foundSome = false

		for (name in files) {
            File inputFile = new File(sourceDirectory, name)

            if (!inputFile.exists()) {
                log.warn 'Input file {} does not exist', inputFile
                continue
            }

            if (!expectedProbes.containsKey(name)) {
                log.warn "File {} not known; I only know about {}", name, expectedProbes.keySet()
            }

            /* the meat: */
            def numProbes = parseGPLFile(inputFile)

            if (numProbes == expectedProbes[name])
                log.debug "Found expected number of probes in {}: {}", name, numProbes
            else if (expectedProbes[name])
                log.warn "Number of probes mismatch in {}. Expected {}, got {}",
                        name, expectedProbes[name], numProbes

            foundSome = true
        }

        if (!foundSome) {
            log.error 'None of the specified input files exist'
            throw new IOException("None of the specified input files exist")
        }
	}


	long parseGPLFile(File gplInput){
		String [] str, header
		def genes = [:]
		boolean isHeaderLine = false
		boolean isAnnotationLine = false

		StringBuffer sb_probeinfo = new StringBuffer()
		StringBuffer sb_snpGeneMap = new StringBuffer()
		StringBuffer sb_snpMap = new StringBuffer()

        log.debug 'Going to parse GPL file {}', gplInput

		long numProbes = 0,
             skippedComments = 0,
             skippedIncomplete = 0
		gplInput.eachLine{
			str = it.split("\t")
			if(str.size() > 14){
				if(it.indexOf("ID") == 0) {
					isHeaderLine = true

					if((gplInput.name.indexOf("GPL2004") > -1) || (gplInput.name.indexOf("GPL2005") > -1)){
						// used for GPL2004 and GPL2005
						log.info str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[5] + "\t" + str[12]
					}

					if((gplInput.name.indexOf("GPL3718") > -1) || (gplInput.name.indexOf("GPL3720") > -1)){
						// used for GPL3718 and GPL3720
						log.info str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[5] + "\t" + str[13]
					}
				}else{
					//if(isHeaderLine && numProbes < 10) {
					if(isHeaderLine) {
						String snpId, rsId, chr, pos
						if((gplInput.name.indexOf("GPL2004") > -1) || (gplInput.name.indexOf("GPL2005") > -1)){
							// used for GPL2004 and GPL2005
							snpId = str[0].trim().toUpperCase()
							rsId = str[1].trim()
							chr = str[2].trim()
							pos = str[5].trim()
							genes = getSNPGeneMapping(str[12])
						}

						if((gplInput.name.indexOf("GPL3718") > -1) || (gplInput.name.indexOf("GPL3720") > -1)){
							// used for GPL3718 and GPL3720
							snpId = str[0].trim()
							rsId = str[2].trim()
							chr = str[3].trim()
							pos = str[6].trim()
							genes = getSNPGeneMapping(str[13])

						}

						if (chr && pos && rsId && !snpId.startsWith('AFFX')) {
							numProbes++
							if(!rsId.equals(null) && (rsId.indexOf("---") == -1))  sb_probeinfo.append(snpId + "\t" + rsId + "\n")
							sb_snpMap.append(chr + "\t" + snpId + "\t0\t" + pos + "\n")
						} else {
                            skippedIncomplete++
                        }

						genes.each { key, val ->
							String [] s = val.split(":")
							String mappingRecord = str[0] + "\t" + key
							sb_snpGeneMap.append(mappingRecord + "\n")
						}
					}
				}
			} else {
                skippedComments++
            }
		}

        log.info "Skipped $skippedComments putative comment lines and " +
                "$skippedIncomplete lines with incomplete information"

		probeInfo.append(sb_probeinfo.toString())
		snpGeneMap.append(sb_snpGeneMap.toString())
		snpMap.append(sb_snpMap.toString())

		return numProbes
	}


	Map getSNPGeneMapping(String associatedGene){

		String [] str, gene
		def mapping = [:]

		if(associatedGene.indexOf("///") >= 0) {
			str = associatedGene.split("///")
			for(int i in 0..str.size()-1) {

				if(str[i].indexOf("//")) {
					gene = str[i].split("//")
					if(gene.size() >= 6 &&  !(gene[5].indexOf("---") >= 0)){
						// 4 -- gene symbol; 5 -- gene id; 6 -- gene description
						//println gene[4] + ":" + gene[5] + ":" + gene[6]
						mapping[gene[5].trim()] = gene[4].trim() + ":" + gene[6].trim()
					}
				}
			}
		}
		return mapping
	}

	void setExpectedProbes(Map expectedProbes){
		this.expectedProbes = expectedProbes
	}

	void setSql(Sql sql){
		this.sql = sql
	}
}
