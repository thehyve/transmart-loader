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
import groovy.sql.Sql
import org.slf4j.Logger

import javax.enterprise.event.Observes
import javax.inject.Inject

class SnpGeneMap {

	@Inject Logger log

	@Inject Sql sql

    @Inject RDMSCompatibility rdmsCompatibility

	void loadSnpGeneMap(@Observes @Step('loadSnpGeneMap_file') StepExecution stepParams) {
        /** see {@link com.recomdata.pipeline.annotation.AnnotationLoader#loadSnpGeneMap()} */
        File snpGeneMap = stepParams['snpGeneMapFile']

        def tempTable = 'tmp_de_snp_gene_map'

        rdmsCompatibility.createTemporaryTable tempTable, 'deapp.de_snp_gene_map'

		// store unique set of SNP ID -> Gene ID
		if (snpGeneMap.size() == 0) {
            throw new EmptyInputFileException("File $snpGeneMap is empty")
        }

        log.info 'Start loading {} into DE_SNP_GENE_MAP...', snpGeneMap

        String qry = """ insert into $tempTable(entrez_gene_id, snp_id, snp_name)
							 select ?, snp_info_id, ?
							 from deapp.de_snp_info where name=? """
        sql.withTransaction {
            sql.withBatch(qry, {stmt ->
                snpGeneMap.eachLine {
                    String[] str = it.split(/\t/)
                    stmt.addBatch([str[1] as Long, str[0], str[0]])
                }
            })
        }
        rdmsCompatibility.analyzeTable tempTable

		loadSnpGeneMap tempTable
	}

	
	void loadSnpGeneMap(String tmpSnpGeneMapTable) {

		log.info "Start loading data into the table DE_SNP_GENE_MAP"

		String qry = """
                INSERT INTO deapp.de_snp_gene_map (
                    snp_id,
                    snp_name,
                    entrez_gene_id )
                SELECT
                    t2.snp_info_id,
                    t1.snp_name,
                    t1.entrez_gene_id
                FROM
                    $tmpSnpGeneMapTable t1
                    INNER JOIN deapp.de_snp_info t2 ON (t1.snp_name = t2.name)
                ${rdmsCompatibility.complementOperator}
                SELECT
                    snp_id,
                    snp_name,
                    entrez_gene_id
                FROM
                    deapp.de_snp_gene_map"""
		def affected = sql.executeUpdate(qry)

		log.info "End loading data into the table DE_SNP_GENE_MAP ({} rows)", affected
	}


// TODO: Refactor with loadSnpGeneMap(String); annotationTable used to a prop
//	void loadSnpGeneMap(Map columnMap){
//
//		log.info "Start loading data into the table DE_SNP_GENE_MAP ... "
//
//		String qry = """insert into de_snp_gene_map nologging (snp_id, snp_name, entrez_gene_id)
//						select t2.snp_info_id, t1.${columnMap["probe"]}, t1.${columnMap["gene_id"]}
//						from $annotationTable t1, de_snp_info t2
//						where t1.${columnMap["probe"]} = t2.name and t1.${columnMap["gene_id"]} not like '---%'
//                        minus
//						select snp_id, snp_name, to_char(entrez_gene_id) from de_snp_gene_map"""
//		sql.execute(qry)
//
//		log.info "End loading data into the table DE_SNP_GENE_MAP ... "
//	}

}
