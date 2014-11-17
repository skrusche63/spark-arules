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

import de.kp.spark.arules.spec.Fields

class TransactionModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElastic(uid:String,rawset:RDD[Map[String,String]]):RDD[(Int,Array[Int])] = {

    val spec = sc.broadcast(Fields.get(uid))
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val user = data(spec.value("user")._1)      

      val group = data(spec.value("group")._1)
      val item  = data(spec.value("item")._1).toInt
      
      (site,user,group,item)
      
    })

    buildSPMF(dataset)

  }
  
  def buildFile(uid:String,rawset:RDD[String]):RDD[(Int,Array[Int])] = {
    
    rawset.map(valu => {
      
      val Array(sid,sequence) = valu.split(",")  
      (sid.toInt,sequence.split(" ").map(_.toInt))
    
    }).cache
    
  }
  
  def buildJDBC(uid:String,rawset:RDD[Map[String,Any]]):RDD[(Int,Array[Int])] = {
        
    val fieldspec = Fields.get(uid)
    val fields = fieldspec.map(kv => kv._2._1).toList    

    val spec = sc.broadcast(fieldspec)
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val user = data(spec.value("user")._1).asInstanceOf[String] 

      val group = data(spec.value("group")._1).asInstanceOf[String]
      val item  = data(spec.value("item")._1).asInstanceOf[Int]
      
      (site,user,group,item)
      
    })
    
    buildSPMF(dataset)

  }

    
  def buildPiwik(uid:String,rawset:RDD[Map[String,Any]]):RDD[(Int,Array[Int])] = {
    
    val rows = rawset.map(row => {
      
      val site = row("idsite").asInstanceOf[Long]
      /* Convert 'idvisitor' into a HEX String representation */
      val idvisitor = row("idvisitor").asInstanceOf[Array[Byte]]     
      val user = new java.math.BigInteger(1, idvisitor).toString(16)

      val group = row("idorder").asInstanceOf[String]
      val item  = row("idaction_sku").asInstanceOf[Long]
      /* 
       * Convert 'site' to String representation to be 
       * harmonized with other data sources
       */
      (site.toString,user,group,item.toInt)
      
    })
    
    buildSPMF(rows)
    
  }

  private def buildSPMF(dataset:RDD[(String,String,String,Int)]):RDD[(Int,Array[Int])] = {
     
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

    val transactions = sc.parallelize(Range.Long(0,ids.count,1),ids.partitions.size)
    ids.zip(transactions).map(valu => (valu._2.toInt,valu._1)).cache()
   
  }
}