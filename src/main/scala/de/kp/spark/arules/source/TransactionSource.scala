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
import de.kp.spark.core.source._

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model.Sources

import de.kp.spark.arules.spec.Fields

/**
 * A TransactionSource is an abstraction layer on top of
 * different physical data source to retrieve a transaction
 * database compatible with the Top-K and Top-KNR algorithm
 */
class TransactionSource(@transient sc:SparkContext) {

  private val config = Configuration
  
  private val itemsetModel = new ItemsetModel(sc)
  private val transactionModel = new TransactionModel(sc)
  
  def transDS(req:ServiceRequest):RDD[(Int,Array[Int])] = {
    
    val source = req.data(Names.REQ_SOURCE)
    source match {
      /* 
       * Discover top k association rules from transaction database persisted 
       * as an appropriate search index from Elasticsearch; the configuration
       * parameters are retrieved from the service configuration 
       */    
      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(config,req)
        transactionModel.buildElastic(req,rawset)
        
      }
      /* 
       * Discover top k association rules from transaction database persisted 
       * as a file on the (HDFS) file system; the configuration parameters are 
       * retrieved from the service configuration  
       */    
      case Sources.FILE => {
        
        val rawset = new FileSource(sc).connect(config.input(0),req)
        transactionModel.buildFile(req,rawset)
        
      }
      /*
       * Retrieve Top-K association rules from transaction database persisted 
       * as an appropriate table from a JDBC database; the configuration parameters 
       * are retrieved from the service configuration
       */
      case Sources.JDBC => {
    
        val fields = Fields.get(req).map(kv => kv._2._1).toList    
        
        val rawset = new JdbcSource(sc).connect(config,req,fields)
        transactionModel.buildJDBC(req,rawset)
        
      }
      /*
       * Retrieve Top-K association rules from transaction database persisted 
       * as parquet file; the configuration parameters are retrieved from the 
       * service configuration
       */
      case Sources.PARQUET => {
    
        val fields = Fields.get(req).map(kv => kv._2._1).toList    
        
        val rawset = new ParquetSource(sc).connect(config.input(0),req,fields)
        transactionModel.buildJDBC(req,rawset)
        
      }
      /*
       * Retrieve Top-K association rules from transaction database persisted 
       * as an appropriate table from a Piwik database; the configuration parameters 
       * are retrieved from the service configuration
       */
      case Sources.PIWIK => {
        
        val rawset = new PiwikSource(sc).connect(config,req)
        transactionModel.buildPiwik(req,rawset)
        
      }
            
      case _ => null
      
   }

  }

  def itemsetDS(req:ServiceRequest):RDD[(String,String,List[Int])] = {
    
    val source = req.data(Names.REQ_SOURCE)
    source match {
      /* 
       * Retrieve most recent itemset from a transaction database persisted
       * as an appropriate search index from Elasticsearch; the configuration
       * parameters are retrieved from the service configuration 
       */    
      case Sources.ELASTIC => {
         
        val rawset = new ElasticSource(sc).connect(config,req)
        itemsetModel.buildElastic(req,rawset)
        
      }
      /* 
       * Retrieve most recent itemset from a transaction database persisted
       * as an appropriate file on the Hadoop file system; the configuration
       * parameters are retrieved from the service configuration 
       */    
      case Sources.FILE => {
        
        val rawset = new FileSource(sc).connect(config.input(0),req)
        itemsetModel.buildFile(req,rawset)
        
      }
      /*
       * Retrieve most recent itemset from a transaction database persisted
       * as an appropriate table from a JDBC database; the parameters are 
       * retrieved from the service configuration
       */
      case Sources.JDBC => {

        val fields = Fields.get(req).map(kv => kv._2._1).toList    
        
        val rawset = new JdbcSource(sc).connect(config,req,fields)
        itemsetModel.buildJDBC(req,rawset)
        
      }
      /*
       * Retrieve most recent itemset from a transaction database persisted
       * as a parquet file; the parameters are retrieved from the service 
       * configuration
       */
      case Sources.PARQUET => {

        val fields = Fields.get(req).map(kv => kv._2._1).toList    
        
        val rawset = new ParquetSource(sc).connect(config.input(0),req,fields)
        itemsetModel.buildJDBC(req,rawset)
        
      }
      /*
       * Retrieve most recent itemset from a transaction database persisted
       * as an appropriate table from a Piwik database; the parameters are 
       * retrieved from the service configuration
       */
      case Sources.PIWIK => {
        
        val rawset = new PiwikSource(sc).connect(config,req)
        itemsetModel.buildPiwik(req,rawset)
        
      }
            
      case _ => null
      
   }
    
  }

}