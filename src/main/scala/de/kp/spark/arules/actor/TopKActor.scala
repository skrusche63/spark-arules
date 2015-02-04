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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._

import de.kp.spark.core.source.ItemSource
import de.kp.spark.core.source.handler.SPMFHandler

import de.kp.spark.arules.{RequestContext,TopK}
import de.kp.spark.arules.model._

import de.kp.spark.arules.spec.ItemSpec

class TopKActor(@transient ctx:RequestContext) extends MLActor(ctx) {
 
  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)
      val missing = (params == null)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        cache.addStatus(req,ResponseStatus.MINING_STARTED)
 
        try {
          
          val source = new ItemSource(ctx.sc,ctx.config,ItemSpec)
          val dataset = SPMFHandler.item2SPMF(source.connect(req))

          findRules(req,dataset,params)
          
        } catch {
          case e:Exception => cache.addStatus(req,ResponseStatus.FAILURE)          
        }
 

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
    
  private def findRules(req:ServiceRequest,dataset:RDD[(Int,Array[Int])],params:(Int,Double)) {
          
    val (k,minconf) = params    
    /*
     * 'total' is the number of transaction and specifies
     * the reference base for the 'support' parameter
     */
    val total = dataset.count()
    
    val rules = TopK.extractRules(dataset,k,minconf).map(rule => {
     
      val antecedent = rule.getItemset1().toList.map(_.toInt)
      val consequent = rule.getItemset2().toList.map(_.toInt)

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new Rule(antecedent,consequent,support,total,confidence)
            
    })
          
    saveRules(req,new Rules(rules))    
    
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