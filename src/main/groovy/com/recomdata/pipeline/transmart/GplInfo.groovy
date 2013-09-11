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
  

package com.recomdata.pipeline.transmart

import com.recomdata.pipeline.util.Step
import com.recomdata.pipeline.util.StepExecution
import groovy.sql.Sql
import org.slf4j.Logger

import javax.enterprise.event.Observes
import javax.inject.Inject
import java.sql.Timestamp

class GplInfo {

    @Inject Logger log

	@Inject Sql sql


    void insertGplInfo(@Observes @Step('insertGplInfo') StepExecution stepParams) {
        insertGplInfo(stepParams.params)
    }

// TODO: implement this variant or change caller
//    void insertGplInfo(String platform, String title, String organism, String markerType) {
//        ...
//    }

	void insertGplInfo(Map data) {

		log.info "Start inserting data into DE_GPL_INFO"

		if (gplInfoExist(data["platform"], data["markerType"])) {
			log.info "Platform {} already exists in DE_GPL_INFO", data["platform"]
            return
		}

        String qry = """insert into deapp.de_gpl_info (
                platform, title, organism, annotation_date, marker_type, release_nbr)
                values(?, ?, ?, ?, ?, ?) """
        sql.execute(qry, [
                data["platform"],
                data["title"],
                data["organism"],
                data["annotationDate"] ?: new Timestamp(System.currentTimeMillis()),
                data["markerType"],
                data["releaseNbr"]
        ])

        log.info "Inserted platform {} inserted into DE_GPL_INFO", data["platform"]
	}


	boolean gplInfoExist(String platform, String markerType) {
		String qry = "select count(*) from deapp.de_gpl_info where platform = ? and marker_type = ?"
		def res = sql.firstRow(qry, [platform, markerType])
        return res[0] > 0
	}
}
