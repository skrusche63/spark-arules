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

      val response = req.task.split(":")(1) match {
        /*
         * This request retrieves those rules where the provided items
         * match the 'antecedent' part of the rule; the items may describe
         * a set of products and the request demands those products that are
         * often purchased with the provided ones.
         */
        case "antecedent" => {

          if (sink.rulesExist(uid) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {    
             
            if (req.data.contains("items") == false) {
               failure(req,Messages.NO_ITEMS_PROVIDED(uid))
             
             } else {
            
               val items = req.data("items").split(",").map(_.toInt).toList
               val rules = sink.rulesByAntecedent(uid,items)
               
               val data = Map("uid" -> uid, "rule" -> rules)
               new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
             
             }
            
          }
          
        }
        /*
         * This request retrieves those rules where the provided items
         * match the 'consequent' part of the rule; the items may describe
         * a set of products for which the purchase rate has to be increased
         * and the request demands those products that should be promoted.
         */
        case "consequent" => {

          if (sink.rulesExist(uid) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {    

            if (req.data.contains("items") == false) {
               failure(req,Messages.NO_ITEMS_PROVIDED(uid))
             
             } else {
            
               val items = req.data("items").split(",").map(_.toInt).toList
               val rules = sink.rulesByConsequent(uid,items)
               
               val data = Map("uid" -> uid, "rule" -> rules)
               new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
             
             }
            
          }
          
        }
        /*
         * This request provides a list of users and demands those products 
         * that have to be recommended to these users; the starting point is
         * the last purchase or transaction of these users
         */
        case "recommendation" => {

          if (sink.multiUserRulesExist(uid) == false) {           
            failure(req, Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {            
            
            val site  = req.data("site")
            val users = req.data("users").split(",").toList
            
            val rules = sink.rulesByUsers(uid,site,users)
            
            val data = Map("uid" -> uid, "recommendation" -> rules)            
            new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
           
          }
          
        }
        /*
         * This request retrieves the association rules mined from the
         * specified data source; no additional data computing is done
         */
        case "rule" => {

          if (sink.rulesExist(uid) == false) {           
            failure(req, Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {            
            
            val rules = sink.rulesAsString(uid)

            val data = Map("uid" -> uid, "rule" -> rules)            
            new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
            
          }
           
        }
        /*
         * This request retrieves the list of those rules that partially match
         * the itemset of the users' latests transaction thereby making sure
         * that the intersection of antecedent and consequent is empty
         */
        case "transaction" => {

          if (sink.multiUserRulesExist(uid) == false) {           
            failure(req, Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {            
            
            val rules = sink.multiUserRulesAsString(uid)

            val data = Map("uid" -> uid, "transaction" -> rules)            
            new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
            
          }
           
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          failure(req,msg)
           
        }
        
      }
           
      origin ! Serializer.serializeResponse(response)
      context.stop(self)
       
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! Serializer.serializeResponse(failure(null,msg))
      context.stop(self)

    }
  
  }
  
}