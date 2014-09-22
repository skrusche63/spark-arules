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

import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.io.{ArrayWritable,MapWritable,NullWritable,Text}

import org.elasticsearch.hadoop.mr.EsInputFormat
import de.kp.spark.arules.spec.FieldSpec

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class ElasticSource(sc:SparkContext) extends Serializable {
 
  /**
   * Read from an Elasticsearch index that contains the items
   * of ecommerce orders or transactions
   */
  def connect(conf:HConf):RDD[(Int,Array[Int])] = {

    val spec = sc.broadcast(FieldSpec.get)

    /* 
     * Connect to Elasticsearch and extract the following fields from the
     * respective search index: site, timestamp, user, order and item
     */
    val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    val rawset = source.map(hit => toMap(hit._2))

    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val timestamp = data(spec.value("timestamp")._1).toLong

      val user = data(spec.value("user")._1)      
      val order = data(spec.value("order")._1)

      val item  = data(spec.value("item")._1).toInt
      
      (site,user,order,timestamp,item)
      
    })
    
    /*
     * Next we convert the dataset into the SPMF format. This requires to
     * group the dataset by 'order', sort items in ascending order and make
     * sure that no item appears more than once in a certain order.
     * 
     * Finally, we organize all items of an order into an array, repartition 
     * them to single partition and assign a unqiue transaction identifier.
     */
    val ids = dataset.groupBy(_._3).map(valu => {

      val sorted = valu._2.map(_._5).toList.distinct.sorted    
      sorted.toArray
    
    }).coalesce(1)

    val index = sc.parallelize(Range.Long(0,ids.count,1),ids.partitions.size)
    ids.zip(index).map(valu => (valu._2.toInt,valu._1)).cache()

  }
  
  /**
   * A helper method to convert a MapWritable into a Map
   */
  private def toMap(mw:MapWritable):Map[String,String] = {
      
    val m = mw.map(e => {
        
      val k = e._1.toString        
      val v = (if (e._2.isInstanceOf[Text]) e._2.toString()
        else if (e._2.isInstanceOf[ArrayWritable]) {
        
          val array = e._2.asInstanceOf[ArrayWritable].get()
          array.map(item => {
            
            (if (item.isInstanceOf[NullWritable]) "" else item.asInstanceOf[Text].toString)}).mkString(",")
            
        }
        else "")
        
    
      k -> v
        
    })
      
    m.toMap
    
  }

}