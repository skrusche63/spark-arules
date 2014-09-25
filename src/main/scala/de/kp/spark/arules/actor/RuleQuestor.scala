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

import akka.actor.{Actor,ActorLogging}

import de.kp.spark.arules.Configuration

import de.kp.spark.arules.model._
import de.kp.spark.arules.util.{JobCache,RuleCache}

import scala.collection.JavaConversions._

class RuleQuestor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task match {
        /*
         * This task retrieves all rules that match the antecendents or
         * consequents provided with this prediction request; the client
         * may then decide how to proceed with this information 
         */
        case "predict" => {

          val resp = if (RuleCache.exists(uid) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {    
             
            val antecedent = req.data.getOrElse("antecedent", null) 
            val consequent = req.data.getOrElse("consequent", null)            

            if (antecedent == null && consequent == null) {
               failure(req,Messages.NO_ANTECEDENTS_OR_CONSEQUENTS_PROVIDED(uid))
             
             } else {
            
               val rules = (if (antecedent != null) {
                 val items = antecedent.split(",").map(_.toInt).toList
                 RuleCache.rulesByAntecedent(uid,items)
               
               } else {
                 val items = consequent.split(",").map(_.toInt).toList
                 RuleCache.rulesByConsequent(uid,items)
                 
               })
               
               val data = Map("uid" -> uid, "rules" -> rules)
               new ServiceResponse(req.service,req.task,data,ARulesStatus.SUCCESS)
             
             }
            
          }
           
          origin ! Serializer.serializeResponse(resp)
          
        }
        
        case "rules" => {
          /*
           * This task retrieves all the association rules detected
           * by a previously finished data mining task
           */
          val resp = if (RuleCache.exists(uid) == false) {           
           failure(req, Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {            
            
            val rules = RuleCache.rules(uid)

            val data = Map("uid" -> uid, "rules" -> rules)            
            new ServiceResponse(req.service,req.task,data,ARulesStatus.SUCCESS)
            
          }
           
          origin ! Serializer.serializeResponse(resp)
           
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          origin ! Serializer.serializeResponse(failure(req,msg))
           
        }
        
      }
      
    }
  
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,ARulesStatus.FAILURE)	
    
  }
  
}