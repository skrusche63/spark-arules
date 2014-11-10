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

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model._

import de.kp.spark.arules.sink.RedisSink

class RuleQuestor extends BaseActor {

  implicit val ec = context.dispatcher
  val sink = new RedisSink()
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task match {

        case "get:antecedent" => {

          val resp = if (sink.rulesExist(uid) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {    
             
            if (req.data.contains("items") == false) {
               failure(req,Messages.NO_ITEMS_PROVIDED(uid))
             
             } else {
            
               val items = req.data("items").split(",").map(_.toInt).toList
               val rules = sink.rulesByAntecedent(uid,items)
               
               val data = Map("uid" -> uid, "rules" -> rules)
               new ServiceResponse(req.service,req.task,data,ARulesStatus.SUCCESS)
             
             }
            
          }
           
          origin ! Serializer.serializeResponse(resp)
          context.stop(self)
          
        }
        
        case "get:consequent" => {

          val resp = if (sink.rulesExist(uid) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {    

            if (req.data.contains("items") == false) {
               failure(req,Messages.NO_ITEMS_PROVIDED(uid))
             
             } else {
            
               val items = req.data("items").split(",").map(_.toInt).toList
               val rules = sink.rulesByConsequent(uid,items)
               
               val data = Map("uid" -> uid, "rules" -> rules)
               new ServiceResponse(req.service,req.task,data,ARulesStatus.SUCCESS)
             
             }
            
          }
           
          origin ! Serializer.serializeResponse(resp)
          context.stop(self)
          
        }
         
        case "get:transaction" => {

          val resp = if (sink.relationsExist(uid) == false) {           
           failure(req, Messages.RELATIONS_DO_NOT_EXIST(uid))
            
          } else {            
            
            val relations = sink.relations(uid)

            val data = Map("uid" -> uid, "rules" -> relations)            
            new ServiceResponse(req.service,req.task,data,ARulesStatus.SUCCESS)
            
          }
           
          origin ! Serializer.serializeResponse(resp)
          context.stop(self)
           
        }
       
        case "get:rule" => {

          val resp = if (sink.rulesExist(uid) == false) {           
           failure(req, Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {            
            
            val rules = sink.rules(uid)

            val data = Map("uid" -> uid, "rules" -> rules)            
            new ServiceResponse(req.service,req.task,data,ARulesStatus.SUCCESS)
            
          }
           
          origin ! Serializer.serializeResponse(resp)
          context.stop(self)
           
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
          origin ! Serializer.serializeResponse(failure(req,msg))
          context.stop(self)
           
        }
        
      }
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! Serializer.serializeResponse(failure(null,msg))
      context.stop(self)

    }
  
  }
  
}