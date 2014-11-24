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

import de.kp.spark.core.source.{ElasticSource,FileSource,JdbcSource}

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model.Sources

import de.kp.spark.arules.spec.Fields

/**
 * A TransactionSource is an abstraction layer on top of
 * different physical data source to retrieve a transaction
 * database compatible with the Top-K and Top-KNR algorithm
 */
class TransactionSource(@transient sc:SparkContext) {

  private val itemsetModel = new ItemsetModel(sc)
  private val transactionModel = new TransactionModel(sc)
  
  def transDS(data:Map[String,String]):RDD[(Int,Array[Int])] = {
    
    val uid = data("uid")
    
    val source = data("source")
    source match {
      /* 
       * Discover top k association rules from transaction database persisted 
       * as an appropriate search index from Elasticsearch; the configuration
       * parameters are retrieved from the service configuration 
       */    
      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(data)
        transactionModel.buildElastic(uid,rawset)
        
      }
      /* 
       * Discover top k association rules from transaction database persisted 
       * as a file on the (HDFS) file system; the configuration parameters are 
       * retrieved from the service configuration  
       */    
      case Sources.FILE => {

        val path = Configuration.file()
         
        val rawset = new FileSource(sc).connect(data,path)
        transactionModel.buildFile(uid,rawset)
        
      }
      /*
       * Retrieve Top-K association rules from transaction database persisted 
       * as an appropriate table from a JDBC database; the configuration parameters 
       * are retrieved from the service configuration
       */
      case Sources.JDBC => {
    
        val fields = Fields.get(uid).map(kv => kv._2._1).toList    
        
        val rawset = new JdbcSource(sc).connect(data,fields)
        transactionModel.buildJDBC(uid,rawset)
        
      }
      /*
       * Retrieve Top-K association rules from transaction database persisted 
       * as an appropriate table from a Piwik database; the configuration parameters 
       * are retrieved from the service configuration
       */
      case Sources.PIWIK => {
        
        val rawset = new PiwikSource(sc).connect(data)
        transactionModel.buildPiwik(uid,rawset)
        
      }
            
      case _ => null
      
   }

  }

  def itemsetDS(data:Map[String,String]):RDD[(String,String,List[Int])] = {
    
    val uid = data("uid")
    
    val source = data("source")
    source match {
      /* 
       * Retrieve most recent itemset from a transaction database persisted
       * as an appropriate search index from Elasticsearch; the configuration
       * parameters are retrieved from the service configuration 
       */    
      case Sources.ELASTIC => {
         
        val rawset = new ElasticSource(sc).connect(data)
        itemsetModel.buildElastic(uid,rawset)
        
      }
      /*
       * Retrieve most recent itemset from a transaction database persisted
       * as an appropriate table from a JDBC database; the parameters are 
       * retrieved from the service configuration
       */
      case Sources.JDBC => {

        val fields = Fields.get(uid).map(kv => kv._2._1).toList    
        
        val rawset = new JdbcSource(sc).connect(data,fields)
        itemsetModel.buildJDBC(uid,rawset)
        
      }
      /*
       * Retrieve most recent itemset from a transaction database persisted
       * as an appropriate table from a Piwik database; the parameters are 
       * retrieved from the service configuration
       */
      case Sources.PIWIK => {
        
        val rawset = new PiwikSource(sc).connect(data)
        itemsetModel.buildPiwik(uid,rawset)
        
      }
            
      case _ => null
      
   }
    
  }

}