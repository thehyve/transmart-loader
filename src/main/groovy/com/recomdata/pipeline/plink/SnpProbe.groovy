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
  

package com.recomdata.pipeline.plink

import com.recomdata.pipeline.database.RDMSCompatibility
import com.recomdata.pipeline.exception.EmptyInputFileException
import com.recomdata.pipeline.util.Step
import com.recomdata.pipeline.util.StepExecution
import org.slf4j.Logger

import javax.enterprise.event.Observes
import javax.inject.Inject
import java.io.File;

import groovy.sql.Sql

class SnpProbe {

	@Inject private Logger log

	@Inject Sql sql

    @Inject RDMSCompatibility rdmsCompatibility

	String annotationTable

	void loadSnpProbe(@Observes @Step('loadSnpProbe_file') StepExecution stepParams) {

        /** see {@link com.recomdata.pipeline.annotation.AnnotationLoader#loadSnpProbe()} */

        File probeInfo = stepParams['snpProbeFile']

        def temporaryTable = "tmp_de_snp_probe"

        rdmsCompatibility.createTemporaryTable temporaryTable, "deapp.de_snp_probe"

		// store unique set of SNP ID -> rs#
		Map rs = [:]
		if (probeInfo.size() > 0) {
			log.info "Start loading {} into DE_SNP_PROBE", probeInfo
			probeInfo.eachLine {
				String [] str = it.split(/\t/)
				rs[str[0]] = str[1]
			}
		} else {
            throw new EmptyInputFileException("File $probeInfo is empty")
		}

		String qry = """insert into $temporaryTable(probe_name, snp_name) values(?, ?) """
		if (probeInfo.size() > 0) {
			sql.withTransaction {
				sql.withBatch(qry, { stmt ->
					rs.each {k, v ->
						stmt.addBatch([k, v])
					}
				})
			}
		}
        rdmsCompatibility.analyzeTable temporaryTable

		loadSnpProbe(temporaryTable)
	}

	
	void loadSnpProbe(String tmpSnpProbeTable){

		log.info "Start loading data into the table DE_SNP_PROBE"
		
		String qry = """
                INSERT INTO deapp.de_snp_probe (
                    probe_name,
                    snp_id,
                    snp_name )
                SELECT
                    PT.probe_name,
                    I.snp_info_id,
                    PT.snp_name
                FROM
                    $tmpSnpProbeTable PT
                    INNER JOIN deapp.de_snp_info I ON (I.name = PT.probe_name)
                WHERE
                    NOT EXISTS (
                        SELECT P.snp_id FROM deapp.de_snp_probe P WHERE P.snp_id = I.snp_info_id)"""
		def affected = sql.executeUpdate qry

		log.info "End loading data into the table DE_SNP_PROBE ({} rows)", affected
    }

	
// TODO: refactor together with loadSnpProbe(String)
//	void loadSnpProbe(Map columnMap){
//
//		log.info "Start loading data into the table DE_SNP_PROBE ... "
//
//		String qry = """insert into de_snp_probe nologging (probe_name, snp_id, snp_name)
//						select distinct t1.${columnMap["probe"]}, t2.snp_info_id, t1.${columnMap["rs"]}
//						from """ + annotationTable + """ t1, de_snp_info t2
//						where upper(t1.${columnMap["probe"]})=upper(t2.name) and t1.${columnMap["rs"]} is not null
//							  and t2.snp_info_id not in (select snp_id from de_snp_probe)"""
//		sql.execute(qry)
//
//		log.info "End loading data into the table DE_SNP_PRObE ... "
//	}
}
