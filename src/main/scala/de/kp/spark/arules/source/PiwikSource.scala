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

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.io.JdbcReader

/**
 * PiwikSource is an extension of the common JdbcSource that holds Piwik specific
 * data about fields and types on the server side for convenience.
 */
class PiwikSource(@transient sc:SparkContext) extends JdbcSource(sc) {
   
  protected val (url,database,user,password) = Configuration.mysql

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

  override def connect(params:Map[String,Any]):RDD[(Int,Array[Int])] = {
    
    /* Retrieve site, start & end date from params */
    val site = params("site").asInstanceOf[Int]
    
    val startdate = params("startdate").asInstanceOf[String]
    val enddate   = params("enddate").asInstanceOf[String]

    val sql = query(database,site.toString,startdate,enddate)
    
    val rawset = new JdbcReader(sc,site,sql).read(LOG_ITEM_FIELDS)    
    val rows = rawset.filter(row => (isDeleted(row) == false)).map(row => {
      
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
  
  override def related(params:Map[String,Any]):RDD[(String,String,List[Int])] = {
    
    /* Retrieve site, start & end date from params */
    val site = params("site").asInstanceOf[Int]
    
    val startdate = params("startdate").asInstanceOf[String]
    val enddate   = params("enddate").asInstanceOf[String]

    val sql = query(database,site.toString,startdate,enddate)
    
    val rawset = new JdbcReader(sc,site,sql).read(LOG_ITEM_FIELDS)    
    val dataset = rawset.filter(row => (isDeleted(row) == false)).map(row => {
      
      val idsite = row("idsite").asInstanceOf[Long]
      /* Convert 'idvisitor' into a HEX String representation */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)

      val group = row("idorder").asInstanceOf[String]
      val item  = row("idaction_sku").asInstanceOf[Long]

      /*
       * Convert server_time into universal timestamp
       */
      val server_time = row("server_time").asInstanceOf[java.sql.Timestamp]
      val timestamp = server_time.getTime()
      
      (idsite,user,group,item,timestamp)
      
    })
    
    dataset.groupBy(v => (v._1,v._2)).map(data => {
      
      val (site,user) = data._1
      val groups = data._2.groupBy(_._3).map(group => {

        /*
         * Every group has a unique timestamp, i.e the timestamp
         * is sufficient to identify a certain group for a user
         */
        val timestamp = group._2.head._5
        val items = group._2.map(_._4.toInt).toList

        (timestamp,items)
        
      }).toList.sortBy(_._1)
      
      /*
       * For merging with the rules discovered, we restrict to the 
       * list of items of the latest group and interpret these as
       * antecedent candidates with respect to the association rules.
       */
     (site.toString,user,groups.last._2)
       
    })
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