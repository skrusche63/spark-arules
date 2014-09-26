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

import de.kp.spark.arules.{Configuration,TopKNR}
import de.kp.spark.arules.source.TransactionSource

import de.kp.spark.arules.model._

import de.kp.spark.arules.redis.RedisCache

class TopKNRActor extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("TopKNRActor",Configuration.spark)

  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")     
      val task = req.task

      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
        /* Register status */
        RedisCache.addStatus(uid,task,ARulesStatus.STARTED)
 
        try {
          
          val source = new TransactionSource(sc)
          /*
           * STEP #1: 
           * Discover rules from the transactional data source
           */
          val dataset = source.get(req.data)
          val rules = if (dataset != null) findRules(uid,task,dataset,params) else null
          /*
           * STEP #2: 
           * Merge rules with transactional data source and build
           * weighted relations thereby filtering those relations
           * below a dynamically provided threshold
           */
          if (rules != null) {
           
            val related = source.related(req.data)               
            if (related != null) {

              val weight = req.data("weight").toDouble
              findRelations(uid,task,related,rules,weight)
           }
            
          }

        } catch {
          case e:Exception => RedisCache.addStatus(uid,task,ARulesStatus.FAILURE)          
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
  
  private def findRelations(uid:String,task:String,related:RDD[(String,String,List[Int])],rules:List[Rule],weight:Double) {

    val bcrules = sc.broadcast(rules)
    val bcweight = sc.broadcast(weight)
              
    val relations = related.map(itemset => {
                
      val (site,user,items) = itemset
      val relations = bcrules.value.map(rule => {
        /*
         * The weight is computed from the intersection ratio
         */
        val intersect = items.intersect(rule.antecedent)
        val ratio = intersect.length.toDouble / items.length
                  
        new Relation(items,rule.consequent,rule.support,rule.confidence,ratio)
                  
      }).filter(r => r.weight > bcweight.value)
                
      new Relations(site,user,relations)
                
    }).collect()
          
    /* Put relations to cache for later requests */
    RedisCache.addRelations(uid,new MultiRelations(relations.toList))
          
    /* Update cache */
    RedisCache.addStatus(uid,task,ARulesStatus.FINISHED)
    
  }
 
  private def findRules(uid:String,task:String,dataset:RDD[(Int,Array[Int])],params:(Int,Double,Int)):List[Rule] = {

    RedisCache.addStatus(uid,task,ARulesStatus.DATASET)
          
    val (k,minconf,delta) = params
    val rules = TopKNR.extractRules(dataset,k,minconf,delta).map(rule => {
     
      val antecedent = rule.getItemset1().toList.map(_.toInt)
      val consequent = rule.getItemset2().toList.map(_.toInt)

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new Rule(antecedent,consequent,support,confidence)
            
    })
          
    /* Put rules to cache */
    RedisCache.addRules(uid,new Rules(rules))
          
    /* Update status */
    RedisCache.addStatus(uid,task,ARulesStatus.RULES)
    
    rules
    
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