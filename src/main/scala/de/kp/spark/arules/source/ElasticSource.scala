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

import de.kp.spark.arules.io.ElasticReader
import de.kp.spark.arules.spec.FieldSpec

class ElasticSource(@transient sc:SparkContext) extends Source(sc) {
 
  /**
   * Retrieve the transaction database to be evaluated from
   * an appropriate Elasticsearch search; the field names that
   * contribute to the transactions are specified by FieldSpec
   */
  override def connect(params:Map[String,Any]):RDD[(Int,Array[Int])] = {
    
    val query = params("query").asInstanceOf[String]
    val resource = params("resource").asInstanceOf[String]

    val spec = sc.broadcast(FieldSpec.get)

    /* 
     * Connect to Elasticsearch and extract the following fields from the
     * respective search index: site, user, group and item
     */
    val rawset = new ElasticReader(sc,resource,query).read
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val user = data(spec.value("user")._1)      

      val group = data(spec.value("group")._1)
      val item  = data(spec.value("item")._1).toInt
      
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

  def related(params:Map[String,Any]):RDD[(String,String,List[Int])] = {

    val query = params("query").asInstanceOf[String]
    val resource = params("resource").asInstanceOf[String]

    val spec = sc.broadcast(FieldSpec.get)

    /* 
     * Connect to Elasticsearch and extract the following fields from the
     * respective search index: site, user, group and item
     */
    val rawset = new ElasticReader(sc,resource,query).read
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val user = data(spec.value("user")._1)      

      val group = data(spec.value("group")._1)
      val item  = data(spec.value("item")._1).toInt

      val timestamp  = data(spec.value("timestamp")._1).toLong
      
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