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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.arules.TopK

import de.kp.spark.arules.source.TransactionSource
import de.kp.spark.arules.model._

import de.kp.spark.arules.redis.RedisCache

class TopKActor(@transient val sc:SparkContext) extends MLActor {
 
  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)
      val missing = (params == null)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        RedisCache.addStatus(req,ARulesStatus.STARTED)
 
        try {
          
          val source = new TransactionSource(sc)
          /*
           * STEP #1: 
           * Discover rules from the transactional data source
           */
          val dataset = source.get(req.data)
          val rules = if (dataset != null) findRules(req,dataset,params) else null
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
              findWeightedRules(req,related,rules,weight)
           }
            
          }
          
        } catch {
          case e:Exception => RedisCache.addStatus(req,ARulesStatus.FAILURE)          
        }
 

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
  
  /**
   * For every (site,user) pair and every discovered association rule, 
   * determine the 'antecedent' intersection ratio and filter those
   * above a user defined threshold, and restrict to those relations,
   * where the transaction 'items' do not intersect with the 'consequents' 
   */  
  private def findWeightedRules(req:ServiceRequest,related:RDD[(String,String,List[Int])],rules:List[Rule],weight:Double) {

    val bcrules = sc.broadcast(rules)
    val bcweight = sc.broadcast(weight)
              
    val weightedRules = related.map(itemset => {
                
      val (site,user,items) = itemset
      val relations = bcrules.value.map(rule => {

        val intersect = items.intersect(rule.antecedent)
        val ratio = intersect.length.toDouble / items.length
                  
        new WeightedRule(items,rule.consequent,rule.support,rule.confidence,ratio)
        /*
         * Restrict to relations, where a) the intersection ratio is above the
         * externally provided threshold ('weight') and b) where no items also
         * appear as consequents of the respective rules
         */
      }).filter(r => (r.weight > bcweight.value) && (r.antecedent.intersect(r.consequent).size == 0))
                
      new WeightedRules(site,user,relations)
                
    }).collect()
          
    saveRelations(req,new MultiRelations(weightedRules.toList))
          
    /* Update RedisCache */
    RedisCache.addStatus(req,ARulesStatus.FINISHED)

    /* Notify potential listeners */
    notify(req,ARulesStatus.FINISHED)
    
  }
  
  private def findRules(req:ServiceRequest,dataset:RDD[(Int,Array[Int])],params:(Int,Double)):List[Rule] = {

    RedisCache.addStatus(req,ARulesStatus.DATASET)
          
    val (k,minconf) = params               
    val rules = TopK.extractRules(dataset,k,minconf).map(rule => {
     
      val antecedent = rule.getItemset1().toList.map(_.toInt)
      val consequent = rule.getItemset2().toList.map(_.toInt)

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new Rule(antecedent,consequent,support,confidence)
            
    })
          
    saveRules(req,new Rules(rules))
          
    /* Update cache */
    RedisCache.addStatus(req,ARulesStatus.RULES)
    
    rules
    
  }
  
  private def properties(req:ServiceRequest):(Int,Double) = {
      
    try {
      
      val k = req.data("k").toInt
      val minconf = req.data("minconf").toDouble
        
      return (k,minconf)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }

}