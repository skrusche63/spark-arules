package de.kp.spark.arules.actor
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

import akka.actor.Actor

import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.{Configuration => HConf}

import de.kp.spark.arules.{Configuration,Rule,TopKNR}
import de.kp.spark.arules.source.{ElasticSource,FileSource,JdbcSource}

import de.kp.spark.arules.model._
import de.kp.spark.arules.util.{JobCache,RuleCache}

class TopKNRActor(req:ServiceRequest) extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("TopKNRActor",Configuration.spark)
  
  private val uid = req.data("uid")    
  JobCache.add(uid,ARulesStatus.STARTED)

  private val params = parameters()

  def receive = {
    
    /*
     * Retrieve Top-KNR association rules from an appropriate 
     * search index from Elasticsearch
     */     
    case req:ElasticRequest => {

      /* Send response to originator of request */
      sender ! response
          
      if (params != null) {

        try {
          
          /* Retrieve data from Elasticsearch */    
          val source = new ElasticSource(sc)
          val dataset = source.connect()

          JobCache.add(uid,ARulesStatus.DATASET)
          
          val (k,minconf,delta) = params     
          findRules(dataset,k,minconf,delta)

        } catch {
          case e:Exception => JobCache.add(uid,ARulesStatus.FAILURE)          
        }
      
      }
      
      sc.stop
      context.stop(self)
      
    }
    
    /*
     * Retrieve Top-KNR association rules from an appropriate 
     * file from the (HDFS) file system
     */
    case req:FileRequest => {

      /* Send response to originator of request */
      sender ! response
          
      if (params != null) {

        try {
    
          /* Retrieve data from the file system */
          val source = new FileSource(sc)
          val dataset = source.connect()

          JobCache.add(uid,ARulesStatus.DATASET)

          val (k,minconf,delta) = params          
          findRules(dataset,k,minconf,delta)

        } catch {
          case e:Exception => JobCache.add(uid,ARulesStatus.FAILURE)
        }
        
      }
      
      sc.stop
      context.stop(self)
      
    }
    /*
     * Retrieve Top-KNR association rules from an appropriate
     * table from a JDBC database 
     */
    case req:JdbcRequest => {

      /* Send response to originator of request */
      sender ! response
          
      if (params != null) {

        try {
    
          val source = new JdbcSource(sc)
          val dataset = source.connect(Map("site" -> req.site, "query" -> req.query))

          JobCache.add(uid,ARulesStatus.DATASET)

          val (k,minconf,delta) = params     
          findRules(dataset,k,minconf,delta)

        } catch {
          case e:Exception => JobCache.add(uid,ARulesStatus.FAILURE)
        }
        
      }
      
      sc.stop
      context.stop(self)
      
    }
    
    case _ => {}
    
  }
  
  private def findRules(dataset:RDD[(Int,Array[Int])],k:Int,minconf:Double,delta:Int) {
          
    val rules = TopKNR.extractRules(dataset,k,minconf,delta).map(rule => {
     
      val antecedent = rule.getItemset1().toList.map(_.toInt)
      val consequent = rule.getItemset2().toList.map(_.toInt)

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new Rule(antecedent,consequent,support,confidence)
            
    })
          
    /* Put rules to RuleCache */
    RuleCache.add(uid,rules)
          
    /* Update JobCache */
    JobCache.add(uid,ARulesStatus.FINISHED)
    
  }
  
  private def parameters():(Int,Double,Int) = {
      
    try {
      val k = req.data("k").toInt
      val minconf = req.data("minconf")toDouble
        
      val delta = req.data("delta").toInt
      return (k,minconf,delta)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
  private def response():ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (params == null) {
      val data = Map("uid" -> uid, "message" -> ARulesMessages.TOP_KNR_MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ARulesStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> ARulesMessages.TOP_KNR_MINING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ARulesStatus.STARTED)	
      
  
    }

  }
  
}