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

import de.kp.spark.arules.spec.FieldSpec

/**
 * JdbcSource is a common connector to Jdbc databases; it uses a common
 * field specification to retrieve the respective transaction database
 * and is independent of the interpretation of an 'item': An item may be
 * an article of a publisher site, a product or service of a retailer site,
 * and also a customer state to specify a certain behavioral state.
 */
class JdbcSource(@transient sc:SparkContext) extends Source(sc) {

  protected val MYSQL_DRIVER   = "com.mysql.jdbc.Driver"
  protected val NUM_PARTITIONS = 1
   
  protected val (url,database,user,password) = Configuration.mysql
  
  protected val fieldspec = FieldSpec.get
  protected val fields = fieldspec.map(kv => kv._2._1).toList
  
  override def connect(params:Map[String,Any]):RDD[(Int,Array[Int])] = {
    
    /* Retrieve site and query from params */
    val site = params("site").asInstanceOf[Int]
    val query = params("query").asInstanceOf[String]
    
    /*
     * Convert field specification into broadcast variable
     */
    val spec = sc.broadcast(FieldSpec.get)

    val rawset = new JdbcReader(sc,site,query).read(fields)
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val user = data(spec.value("user")._1).asInstanceOf[String] 

      val group = data(spec.value("group")._1).asInstanceOf[String]
      val item  = data(spec.value("item")._1).asInstanceOf[Int]
      
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
    val ids = dataset.groupBy(_._3).map(valu => {

      val sorted = valu._2.map(_._4).toList.distinct.sorted    
      sorted.toArray
    
    }).coalesce(1)

    val index = sc.parallelize(Range.Long(0,ids.count,1),ids.partitions.size)
    ids.zip(index).map(valu => (valu._2.toInt,valu._1)).cache()

  }
   
  override def related(params:Map[String,Any]):RDD[(String,String,List[Int])] = {

    /* Retrieve site and query from params */
    val site = params("site").asInstanceOf[Int]
    val query = params("query").asInstanceOf[String]
    
    /*
     * Convert field specification into broadcast variable
     */
    val spec = sc.broadcast(FieldSpec.get)

    val rawset = new JdbcReader(sc,site,query).read(fields)
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val user = data(spec.value("user")._1).asInstanceOf[String]      

      val group = data(spec.value("group")._1).asInstanceOf[String]
      val item  = data(spec.value("item")._1).asInstanceOf[Int]

      val timestamp  = data(spec.value("timestamp")._1).asInstanceOf[Long]
      
      (site,user,group,item,timestamp)
      
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
     (site,user,groups.last._2)
       
    })
  }

}