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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model._

import de.kp.spark.arules.sink.RedisSink

class RuleQuestor extends BaseActor {

  implicit val ec = context.dispatcher
  val sink = new RedisSink()
  
  def receive = {
    
    /*
     * The Association Analysis engines distinguish two types of requests: (a) rule oriented requests 
     * that exclusively refer to the detected association rules, and, (b) requests that combine these
     * rules with other data sources. Actually, this other data source is the last user transaction
     * within the overall transaction database.
     * 
     * (a) rule oriented requests: 'antecedent', 'consequent' and 'rule'
     * 
     * (b) combined requests: 'transaction'
     * 
     * The latter request combines (user --> items) with (item --> item) rules and enables to reach
     * related items for a certain user. This information is used as a basis for recommendations.
     */
    
    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)

      val Array(task,topic) = req.task.split(":")
      val response = topic match {
        /*
         * This request retrieves those rules where the provided items
         * match the 'antecedent' part of the rule; the items may describe
         * a set of products and the request demands those products that are
         * often purchased with the provided ones.
         */
        case "antecedent" => {

          if (sink.rulesExist(req) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {    
             
            if (req.data.contains(Names.REQ_ITEMS) == false) {
               failure(req,Messages.NO_ITEMS_PROVIDED(uid))
             
             } else {
            
               val items = req.data(Names.REQ_ITEMS).split(",").map(_.toInt).toList
               val rules = sink.rulesByAntecedent(req,items)
               
               val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> rules)
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

          if (sink.rulesExist(req) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {    

            if (req.data.contains(Names.REQ_ITEMS) == false) {
               failure(req,Messages.NO_ITEMS_PROVIDED(uid))
             
             } else {
            
               val items = req.data(Names.REQ_ITEMS).split(",").map(_.toInt).toList
               val rules = sink.rulesByConsequent(req,items)
               
               val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> rules)
               new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
             
             }
            
          }
          
        }
        case "crule" => {
          /*
           * This request retrieves a derived version of association with a single
           * consequent and the respective weight with respect to original rules;
           * these classifier rules can be used to feed classifiers.
           */ 
          if (sink.rulesExist(req) == false) {           
            failure(req, Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {       
            
            val crules = sink.rulesAsList(req).flatMap(rule => {
            
              val ratio = 1.toDouble / rule.consequent.length
              rule.consequent.map(item => CRule(rule.antecedent,item,rule.support,rule.confidence,ratio))
              
            })
               
            val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> Serializer.serializeCRules(CRules(crules)))
            new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
            
          }
         
        }
        /*
         * This request retrieves the association rules mined from the
         * specified data source; no additional data computing is done
         */
        case "rule" => {

          if (sink.rulesExist(req) == false) {           
            failure(req, Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {            
            
            val rules = sink.rulesAsString(req)

            val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> rules)            
            new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
            
          }
           
        }
        /*
         * This request retrieves the list of those rules that partially match
         * the itemset of the users' last transaction thereby making sure that
         * the intersection of antecedent and consequent is empty.
         */
        case "transaction" => {

          if (sink.userRulesExist(uid) == false) {           
            failure(req, Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {            
            
            val rules = if (req.data.contains(Names.REQ_SITE) && req.data.contains(Names.REQ_USERS)) {
            
              val site  = req.data(Names.REQ_SITE)
              val users = req.data(Names.REQ_USERS).split(",").toList
            
              sink.rulesByUsers(uid,site,users)
            
            } else sink.rulesByAllUsers(uid)

            val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> rules)            
            new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
            
          }
           
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          failure(req,msg)
           
        }
        
      }
           
      origin ! response
      context.stop(self)
       
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }
  
}