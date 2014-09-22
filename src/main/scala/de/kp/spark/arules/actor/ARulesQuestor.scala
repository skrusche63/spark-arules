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

class ARulesQuestor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task match {
        
        case "predict" => {

          val resp = if (RuleCache.exists(uid) == false) {           
            failure(req,ARulesMessages.RULES_DO_NOT_EXIST(uid))
            
          } else {    
             
            val antecedent = req.data.getOrElse("antecedent", null)            
             if (antecedent == null) {
               failure(req,ARulesMessages.ANTECEDENTS_DO_NOT_EXIST(uid))
             
             } else {
            
               
              val consequent = RuleCache.consequent(uid,antecedent.split(",").map(_.toInt).toList).mkString(",")
              val data = Map("uid" -> uid, "consequent" -> consequent)
            
              new ServiceResponse(req.service,req.task,data,ARulesStatus.SUCCESS)
             
             }
            
          }
           
          origin ! ARulesModel.serializeResponse(resp)
          
        }
        
        case "rules" => {
          /*
           * Rules MUST exist then return computed rules
           */
          val resp = if (RuleCache.exists(uid) == false) {           
           failure(req, ARulesMessages.RULES_DO_NOT_EXIST(uid))
            
          } else {            
            
            val rules = RuleCache.rules(uid).map(rule => rule.toJSON).mkString(",")
            val data = Map("uid" -> uid, "rules" -> rules)
            
            new ServiceResponse(req.service,req.task,data,ARulesStatus.SUCCESS)
            
          }
           
          origin ! ARulesModel.serializeResponse(resp)
           
        }
        
        case _ => {
          
          val msg = ARulesMessages.TASK_IS_UNKNOWN(uid,req.task)
          origin ! ARulesModel.serializeResponse(failure(req,msg))
           
        }
        
      }
      
    }
  
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,ARulesStatus.FAILURE)	
    
  }
  
}