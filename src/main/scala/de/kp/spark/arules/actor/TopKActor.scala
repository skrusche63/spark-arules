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

import de.kp.spark.arules.{Configuration,Rule,TopK}
import de.kp.spark.arules.source.{ElasticSource,FileSource}

import de.kp.spark.arules.model._
import de.kp.spark.arules.util.{JobCache,RuleCache}

class TopKActor(jobConf:JobConf) extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("TopKActor",Configuration.spark)      
  
  private val uid = jobConf.get("uid").get.asInstanceOf[String]     
  JobCache.add(uid,ARulesStatus.STARTED)

  private val params = parameters()

  private val response = if (params == null) {
    val message = ARulesMessages.TOP_K_MISSING_PARAMETERS(uid)
    new ARulesResponse(uid,Some(message),None,None,ARulesStatus.FAILURE)
  
  } else {
     val message = ARulesMessages.TOP_K_MINING_STARTED(uid)
     new ARulesResponse(uid,Some(message),None,None,ARulesStatus.STARTED)
    
  }
  
  def receive = {
    
    /*
     * Retrieve Top-K association rules from an appropriate index from Elasticsearch
     */     
    case req:ElasticRequest => {

      /* Send response to originator of request */
      sender ! response
          
      if (params != null) {

        try {
          
          val conf = Configuration.elastic
          conf.set("es.fields",req.fields)
          
          /* Retrieve data from Elasticsearch */    
          val source = new ElasticSource(sc)
          val dataset = source.connect(conf)

          JobCache.add(uid,ARulesStatus.DATASET)
          
          val (k,minconf) = params     
          findRules(dataset,k,minconf)

        } catch {
          case e:Exception => JobCache.add(uid,ARulesStatus.FAILURE)          
        }
      
      }
      
      sc.stop
      context.stop(self)
      
    }
    
    /*
     * Retrieve Top-K association rules from an appropriate file from the
     * (HDFS) file system; the file MUST have a specific file format;
     * 
     * actually it MUST be ensured by the client application that such
     * a file exists in the right format
     */
    case req:FileRequest => {

      /* Send response to originator of request */
      sender ! response
          
      if (params != null) {

        try {
    
          /* Retrieve data from the file system */
          val source = new FileSource(sc)
          
          val path = req.path
          val dataset = source.connect(path)

          JobCache.add(uid,ARulesStatus.DATASET)

          val (k,minconf) = params          
          findRules(dataset,k,minconf)

        } catch {
          case e:Exception => JobCache.add(uid,ARulesStatus.FAILURE)
        }
        
      }
      
      sc.stop
      context.stop(self)
      
    }
    
    case _ => {}
    
  }
  
  private def findRules(dataset:RDD[(Int,Array[String])],k:Int,minconf:Double) {
          
    val rules = TopK.extractRules(dataset,k,minconf).map(rule => {
     
      val antecedent = rule.getItemset1().toList
      val consequent = rule.getItemset2().toList

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new Rule(antecedent,consequent,support,confidence)
            
    })
          
    /* Put rules to RuleCache */
    RuleCache.add(uid,rules)
          
    /* Update JobCache */
    JobCache.add(uid,ARulesStatus.FINISHED)
    
  }
  
  private def parameters():(Int,Double) = {
      
    try {
      val k = jobConf.get("k").get.asInstanceOf[Int]
      val minconf = jobConf.get("minconf").get.asInstanceOf[Double]
        
      return (k,minconf)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
}