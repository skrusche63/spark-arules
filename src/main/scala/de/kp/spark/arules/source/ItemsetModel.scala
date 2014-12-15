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

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.arules.spec.Fields

/**
 * The ItemsetModel supports association rules with an antecedent part
 * that specifies the latest transaction
 */
class ItemsetModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElastic(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[(String,String,List[Int])] = {

    val spec = sc.broadcast(Fields.get(req))
    val dataset = rawset.map(data => {
      
      val site = data(spec.value(Names.SITE_FIELD)._1)
      val user = data(spec.value(Names.USER_FIELD)._1)      

      val group = data(spec.value(Names.GROUP_FIELD)._1)
      val item  = data(spec.value(Names.ITEM_FIELD)._1).toInt

      val timestamp  = data(spec.value(Names.TIMESTAMP_FIELD)._1).toLong
      
      (site,user,group,item,timestamp)
      
    })

    buildItemset(dataset)
  
  }
  
  def buildFile(req:ServiceRequest,rawset:RDD[String]):RDD[(String,String,List[Int])] = {
    
    val dataset = rawset.map(valu => {
      
      val Array(site,user,group,item,timestamp) = valu.split(",")  
      (site,user,group,item.toInt,timestamp.toLong)
    
    })

    buildItemset(dataset)
    
  }
  
  def buildJDBC(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[(String,String,List[Int])] = {
    
    val fieldspec = Fields.get(req)
    val fields = fieldspec.map(kv => kv._2._1).toList    

    val spec = sc.broadcast(fieldspec)
    val dataset = rawset.map(data => {
      
      val site = data(spec.value(Names.SITE_FIELD)._1).asInstanceOf[String]
      val user = data(spec.value(Names.USER_FIELD)._1).asInstanceOf[String]      

      val group = data(spec.value(Names.GROUP_FIELD)._1).asInstanceOf[String]
      val item  = data(spec.value(Names.ITEM_FIELD)._1).asInstanceOf[Int]

      val timestamp  = data(spec.value(Names.TIMESTAMP_FIELD)._1).asInstanceOf[Long]
      
      (site,user,group,item,timestamp)
      
    })

    buildItemset(dataset)
    
  }

  def buildParquetC(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[(String,String,List[Int])] = {
    
    val fieldspec = Fields.get(req)
    val fields = fieldspec.map(kv => kv._2._1).toList    

    val spec = sc.broadcast(fieldspec)
    val dataset = rawset.map(data => {
      
      val site = data(spec.value(Names.SITE_FIELD)._1).asInstanceOf[String]
      val user = data(spec.value(Names.USER_FIELD)._1).asInstanceOf[String]      

      val group = data(spec.value(Names.GROUP_FIELD)._1).asInstanceOf[String]
      val item  = data(spec.value(Names.ITEM_FIELD)._1).asInstanceOf[Int]

      val timestamp  = data(spec.value(Names.TIMESTAMP_FIELD)._1).asInstanceOf[Long]
      
      (site,user,group,item,timestamp)
      
    })

    buildItemset(dataset)
    
  }
 
  def buildPiwik(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[(String,String,List[Int])] = {

    val dataset = rawset.map(row => {
      
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
      
      (idsite.toString,user,group,item.toInt,timestamp)
      
    })
 
    buildItemset(dataset)
    
  }
  
  private def buildItemset(dataset:RDD[(String,String,String,Int,Long)]):RDD[(String,String,List[Int])] = {
    
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