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

/**
 * PiwikSource is an extension of the common JdbcSource that holds Piwik specific
 * data about fields and types on the server side for convenience.
 */
class PiwikSource(@transient sc:SparkContext) extends JdbcSource(sc) {

  override def connect(params:Map[String,Any]):RDD[(Int,Array[Int])] = {
    
    /* Retrieve site, start & end date from params */
    val site = params("site").asInstanceOf[Int]
    
    val startdate = params("startdate").asInstanceOf[String]
    val enddate   = params("enddate").asInstanceOf[String]

    val sql = query(database,site.toString,startdate,enddate)
    val rows = readTable(site,sql).filter(row => (isDeleted(row) == false)).map(row => {
      
      val site = row("idsite").asInstanceOf[Long]
      /* Convert 'idvisitor' into a HEX String representation */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)

      val group = row("idorder").asInstanceOf[String]
      val item  = row("idaction_sku").asInstanceOf[Long]
      
      (site,user,group,item)
      
    })
    
    /*
     * Next we convert the dataset into the SPMF format. This requires to
     * group the dataset by 'group', sort items in ascending order and make
     * sure that no item appears more than once in a certain order.
     * 
     * Finally, we organize all items of an order into an array, repartition 
     * them to single partition and assign a unqiue transaction identifier.
     */
    val ids = rows.groupBy(_._3).map(data => {

      val sorted = data._2.map(_._4.toInt).toList.distinct.sorted    
      sorted.toArray
    
    }).coalesce(1)

    val index = sc.parallelize(Range.Long(0,ids.count,1),ids.partitions.size)
    ids.zip(index).map(valu => (valu._2.toInt,valu._1)).cache()

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