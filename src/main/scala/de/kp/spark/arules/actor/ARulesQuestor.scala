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

class ARulesActor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {

    case req:ARulesRequest => {
      
      val origin = sender    
      
      val (uid,task) = (req.uid,req.task)
      task match {
        
        case "consequent" => {

          val resp = if (RuleCache.exists(uid) == false) {           
            val message = ARulesMessages.RULES_DO_NOT_EXIST(uid)
            new ARulesResponse(uid,Some(message),None,None,ARulesStatus.FAILURE)
            
          } else {    
             
            val antecedent = req.antecedent.getOrElse(null)
             if (antecedent == null) {
               val message = ARulesMessages.ANTECEDENTS_DO_NOT_EXIST(uid)
               new ARulesResponse(uid,Some(message),None,None,ARulesStatus.FAILURE)
             
             } else {
            
              val consequent = RuleCache.consequent(uid,antecedent.items)
              new ARulesResponse(uid,None,None,Some(consequent),ARulesStatus.SUCCESS)
             
             }
            
          }
           
          origin ! ARulesModel.serializeResponse(resp)
          
        }
        
        case "rules" => {
          /*
           * Rules MUST exist then return computed rules
           */
          val resp = if (RuleCache.exists(uid) == false) {           
            val message = ARulesMessages.RULES_DO_NOT_EXIST(uid)
            new ARulesResponse(uid,Some(message),None,None,ARulesStatus.FAILURE)
            
          } else {            
            val rules = RuleCache.rules(uid)
            new ARulesResponse(uid,None,Some(rules),None,ARulesStatus.SUCCESS)
            
          }
           
          origin ! ARulesModel.serializeResponse(resp)
           
        }
        
      }
      
    }
  
  }
  
}