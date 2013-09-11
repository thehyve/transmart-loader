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

import com.recomdata.pipeline.annotation.AnnotationLoader
import com.recomdata.pipeline.database.RDMSCompatibility
import com.recomdata.pipeline.exception.EmptyInputFileException
import com.recomdata.pipeline.util.Step
import com.recomdata.pipeline.util.StepExecution
import groovy.sql.Sql
import org.slf4j.Logger

import javax.enterprise.event.Observes
import javax.inject.Inject

class SnpInfo {

    @Inject Logger log

	@Inject Sql sql

    @Inject RDMSCompatibility rdmsCompatibility

	void loadSnpInfo(@Observes @Step('loadSnpInfo_file') StepExecution stepParams){

        /** see {@link AnnotationLoader#loadSnpInfo()} */

        File snpMapFile = stepParams['snpMapFile']

		String qry

        def temporaryTable = 'tmp_de_snp_info'

        rdmsCompatibility.createTemporaryTable temporaryTable, 'deapp.de_snp_info'

		qry = "insert into $temporaryTable(name, chrom, chrom_pos) values(?, ?, ?)"
		if (snpMapFile.size() > 0) {
			sql.withTransaction {

				log.info("Start loading " + snpMapFile.toString() + " into TMP_DE_SNP_INFO")

				sql.withBatch(qry, {stmt ->
					snpMapFile.eachLine {
						String [] str = it.split(/\t/)
						stmt.addBatch([str[1], str[0], str[3] as Long])
					}
				})
			}
            rdmsCompatibility.analyzeTable temporaryTable
		} else {
            throw new EmptyInputFileException("File $snpMapFile is empty")
		}


        /* transfer to final table */
		loadSnpInfo(temporaryTable)
	}


	void loadSnpInfo(String tmpSnpInfoTable){

		log.info "Start loading data into the table DE_SNP_INFO"

		String qry = """insert into deapp.de_snp_info(name, chrom, chrom_pos)
						select distinct name, chrom, chrom_pos
						from $tmpSnpInfoTable T
						where not exists (select S.name from deapp.de_snp_info S where S.name = T.name)"""
        def numRows = sql.executeUpdate(qry)

		log.info "End loading data into the table DE_SNP_INFO ({} rows)", numRows
	}
}
