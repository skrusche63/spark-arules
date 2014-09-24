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

import de.kp.spark.arules.{Configuration,Rule,TopKNR}
import de.kp.spark.arules.source.{ElasticSource,FileSource,JdbcSource,PiwikSource}

import de.kp.spark.arules.model._
import de.kp.spark.arules.util.{JobCache,RuleCache}

class TopKNRActor extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("TopKNRActor",Configuration.spark)

  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")     
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
        /* Register status */
        JobCache.add(uid,ARulesStatus.STARTED)
 
        try {
          
          val source = req.data("source")
          val dataset = source match {
            
            /* 
             * Discover top k association rules from transaction database persisted 
             * as an appropriate search index from Elasticsearch; the configuration
             * parameters are retrieved from the service configuration 
             */    
            case Sources.ELASTIC => new ElasticSource(sc).connect()
            /* 
             * Discover top k association rules from transaction database persisted 
             * as a file on the (HDFS) file system; the configuration parameters are 
             * retrieved from the service configuration  
             */    
            case Sources.FILE => new FileSource(sc).connect()
            /*
             * Retrieve Top-K association rules from transaction database persisted 
             * as an appropriate table from a JDBC database; the configuration parameters 
             * are retrieved from the service configuration
             */
            case Sources.JDBC => new JdbcSource(sc).connect(req.data)
             /*
             * Retrieve Top-K association rules from transaction database persisted 
             * as an appropriate table from a Piwik database; the configuration parameters 
             * are retrieved from the service configuration
             */
            case Sources.PIWIK => new PiwikSource(sc).connect(req.data)
            
          }

          JobCache.add(uid,ARulesStatus.DATASET)
          
          val (k,minconf,delta) = params     
          findRules(uid,dataset,k,minconf,delta)

        } catch {
          case e:Exception => JobCache.add(uid,ARulesStatus.FAILURE)          
        }
 

      }
      
      sc.stop
      context.stop(self)
          
    }
    
    case _ => {
      
      sc.stop
      context.stop(self)
      
    }
    
  }
 
  private def findRules(uid:String,dataset:RDD[(Int,Array[Int])],k:Int,minconf:Double,delta:Int) {
          
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
  
  private def properties(req:ServiceRequest):(Int,Double,Int) = {
      
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
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.TOP_KNR_MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ARulesStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.TOP_KNR_MINING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ARulesStatus.STARTED)	
  
    }

  }
  
}