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

import com.recomdata.pipeline.entrypoint.Pipeline
import com.recomdata.pipeline.plink.SnpGeneMap
import com.recomdata.pipeline.plink.SnpInfo
import com.recomdata.pipeline.plink.SnpProbe
import com.recomdata.pipeline.transmart.GplInfo
import com.recomdata.pipeline.util.FileRef
import com.recomdata.pipeline.util.Preliminary
import com.recomdata.pipeline.util.StepExecution
import groovy.sql.Sql
import org.slf4j.Logger

import javax.enterprise.event.Event
import javax.enterprise.event.Observes
import javax.inject.Inject

class AnnotationLoader {

    private static final String KEY_DESTINATION_DIRECTORY = "destination_directory"
    private static final String KEY_PROBE_INFO_FILE = "probe_info_file"
    private static final String KEY_SNP_GENE_MAP_FILE = "snp_gene_map_file"
    private static final String KEY_SNP_MAP_FILE = "snp_map_file"
    private static final String KEY_SOURCE_DIRECTORY = "source_directory"
    private static final String KEY_INPUT_FILE = "input_file"

    @Inject private Logger log

    @Inject @FileRef('Annotation') private Properties props

    @Inject @FileRef('Common') private Properties common

    @Inject @Preliminary Event<StepExecution> stepEvent

    final static expectedProbes = [
            "GPL2005-3532.txt":  59015,
            "GPL2004-3450.txt":  57299,
            "GPL3718-44346.txt": 2622264,
            "GPL3720-22610.txt": 238304
    ]

	public main(@Observes @Pipeline('annotation') List<String> args) {

		loadGPL expectedProbes
		loadSnpInfo()
		loadSnpProbe()
		loadSnpGeneMap()
		loadGplInfo()

        //TODO: from here on, not ported; not working
		loadAffymetrix()
		loadTaxonomy()
		loadGeneInfo()
		loadGxGPL()
	}



	void loadGplInfo() {
        /** see {@link GplInfo#insertGplInfo(com.recomdata.pipeline.util.StepExecution)} */
        stepEvent.fire new StepExecution(
                skip           : props.getAsBoolean('skip_de_gpl_info'),
                stepType       : 'insertGplInfo',

                platform       : props.get('platform'),
                title          : props.get('title'),
                organism       : props.get('organism'),
                markerType     : props.get('marker_type'),
        )
	}


	void loadSnpInfo() {
        /** see {@link SnpInfo#loadSnpInfo(com.recomdata.pipeline.util.StepExecution)} */
        // load de_snp_info
        stepEvent.fire new StepExecution(
                skip       : props.getAsBoolean('skip_de_snp_info'),
                stepType   : 'loadSnpInfo_file',

                snpMapFile : new File(props.get(KEY_DESTINATION_DIRECTORY), props.get(KEY_SNP_MAP_FILE)),
        )
	}

	void loadSnpProbe() {
        /** see {@link SnpProbe#loadSnpProbe(com.recomdata.pipeline.util.StepExecution)} */
        // load de_snp_probe
        stepEvent.fire new StepExecution(
                skip         : props.getAsBoolean('skip_de_snp_probe'),
                stepType     : 'loadSnpProbe_file',

                snpProbeFile : new File(props.get(KEY_DESTINATION_DIRECTORY), props.get(KEY_PROBE_INFO_FILE)),
        )
    }


	void loadSnpGeneMap() {
        /** see {@link SnpGeneMap#loadSnpGeneMap(com.recomdata.pipeline.util.StepExecution)} */
        stepEvent.fire new StepExecution(
                skip           : props.getAsBoolean('skip_de_snp_gene_map'),
                stepType       : 'loadSnpGeneMap_file',

                snpGeneMapFile : new File(props.get(KEY_DESTINATION_DIRECTORY), props.get(KEY_SNP_GENE_MAP_FILE)),
        )
	}

    private File getOutputFile(String key) {
        File file = new File(props.get(KEY_DESTINATION_DIRECTORY), props.get(key))

        if (file.size()) {
            log.warn 'The file {} was not empty; recreating anyway', file
            if (!file.delete()) {
                log.error 'Could not delete file {}', file
                throw new IOException("Could not delete file $file")
            }
        }

        file
    }

	void loadGPL(Map expectedProbes) {

        /** see {@link GPLReader#processGPLs(com.recomdata.pipeline.util.StepExecution)} */
        stepEvent.fire new StepExecution(
                skip            : props.getAsBoolean('skip_gpl_annotation_loader'),
                stepType        : 'processGPLs',

                expectedProbes  : expectedProbes,
                inputFileName   : props.get(KEY_INPUT_FILE),

                probeInfo       : getOutputFile(KEY_PROBE_INFO_FILE),
                snpMap          : getOutputFile(KEY_SNP_MAP_FILE),
                snpGeneMap      : getOutputFile(KEY_SNP_GENE_MAP_FILE),
                sourceDirectory : new File(props.get(KEY_SOURCE_DIRECTORY)),
        )
	}


	void loadAffymetrix() {

        //TODO: not ported; not working
		if(props.get("skip_gx_annotation_loader").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Affymetrix GX annotation file(s)"
		}else{

			File annotationSource = new File(props.get("annotation_source"))

			//File annotationSource = new File("C:/Customers/MPI/Affymetrix/Affymetrix.HG-U133_Plus_2.txt")
			//File annotationSource = new File("C:/Customers/MPI/Affymetrix/Affymetrix.HG-U133A.txt")
			//File annotationSource = new File("C:/Customers/MPI/Affymetrix/Affymetrix.Celegans.txt")

			AffymetrixNetAffyGxAnnotation a = new AffymetrixNetAffyGxAnnotation()
			a.setSql(biomart)
			a.setAnnotationTable(props.get("annotation_table"))

			if(props.get("recreate_annotation_table").toString().toLowerCase().equals("yes")){
				log.info "Start recreating annotation table ${props.get("annotation_table")} for Affymetrix GX annotation file(s)"
				a.createAnnotationTable()
			}

			a.loadAffymetrixs(annotationSource)
		}
	}


	void loadTaxonomy() {

        //TODO: not ported; not working
		if(props.get("skip_taxonomy_name").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Taxonomy name"
		}else{
			File taxonomy = new File("C:/Customers/NCBI/Taxonomy/names.dmp")

			Taxonomy t = new Taxonomy()
			t.setBiomart(biomart)
			t.setTaxonomyTable(props.get("taxonomy_name_table"))

			t.createTaxonomyTable()
			t.loadTaxonomy(taxonomy)
		}
	}


	void loadGeneInfo() {

        //TODO: not ported; not working
		if(props.get("skip_gene_info").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Gene Info"
		}else{
			File geneInfo = new File("C:/Customers/NCBI/Taxonomy/gene_info")

			GeneInfo gi = new GeneInfo()
			gi.setBiomart(biomart)
			gi.setGeneInfoTable(props.get("gene_info_table"))
			////gi.createGeneInfoTable()
			//gi.loadGeneInfo(geneInfo)
		}
	}


	void loadGxGPL() {

        //TODO: not ported; not working
		if(props.get("skip_gx_gpl_loader").toString().toLowerCase().equals("yes")){
			log.info "Skip loading GPL GX annotation file(s)"
		}else{


			GexGPL gpl = new GexGPL()
			gpl.setSql(biomart)
			gpl.setAnnotationTable(props.get("annotation_table"))

			if(props.get("recreate_annotation_table").toString().toLowerCase().equals("yes")){
				log.info "Start recreating annotation table ${props.get("annotation_table")} for GPL GX annotation file(s)"
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

}
