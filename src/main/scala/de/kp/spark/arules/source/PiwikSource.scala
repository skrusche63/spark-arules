package de.kp.spark.arules.source
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-ARULES project
* (https://github.com/skrusche63/spark-arules).
* 
* Spark-ARULES is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-ARULES is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-ARULES. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Configuration
import de.kp.spark.core.model._

import de.kp.spark.core.source.JdbcSource
import de.kp.spark.core.io.JdbcReader

/**
 * PiwikSource is an extension of the common JdbcSource that holds Piwik specific
 * data about fields and types on the server side for convenience.
 */
class PiwikSource(@transient sc:SparkContext) extends JdbcSource(sc) {

  private val LOG_ITEM_FIELDS = List(
      "idsite",
      "idvisitor",
      "server_time",
      "idorder",
      /*
       * idaction_xxx are references to unique entries into the piwik_log_action table, 
       * i.e. two items with the same SKU do have the same idaction_sku; the idaction_sku
       * may therefore directly be used as an item identifier
       */
      "idaction_sku",
      "price",
      "quantity",
      "deleted")

  override def connect(config:Configuration,req:ServiceRequest,fields:List[String]=List.empty[String]):RDD[Map[String,Any]] = {
    
    /* Retrieve site, start & end date from params */
    val site = req.data("site").asInstanceOf[Int]
    
    val startdate = req.data("startdate").asInstanceOf[String]
    val enddate   = req.data("enddate").asInstanceOf[String]
   
    val (url,database,user,password) = config.mysql
    val sql = query(database,site.toString,startdate,enddate)
    
    new JdbcReader(sc,config,site,sql).read(LOG_ITEM_FIELDS)    

  }
  
  /**
   * A commerce item may be deleted from a certain order
   */
  private def isDeleted(row:Map[String,Any]):Boolean = row("deleted").asInstanceOf[Boolean]

  /*
   * Table: piwik_log_conversion_item
   */
  private def query(database:String,site:String,startdate:String,enddate:String) = String.format("""
    SELECT * FROM %s.piwik_log_conversion_item WHERE idsite >= %s AND idsite <= %s AND server_time > '%s' AND server_time < '%s'
    """.stripMargin, database, site, site, startdate, enddate) 

}