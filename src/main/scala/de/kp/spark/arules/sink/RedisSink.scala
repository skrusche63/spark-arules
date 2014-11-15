package de.kp.spark.arules.sink
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

import java.util.Date

import de.kp.spark.arules.model._
import de.kp.spark.arules.redis.RedisClient

import scala.collection.JavaConversions._

class RedisSink {

  val client  = RedisClient()
  val service = "arules"

  def addRelations(req:ServiceRequest,relations:MultiRelations) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "relation:" + service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeMultiRelations(relations)
    
    client.zadd(k,timestamp,v)
    
  }

  def addRules(req:ServiceRequest, rules:Rules) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "rule:" + service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeRules(rules)
    
    client.zadd(k,timestamp,v)
    
  }
 
  def relationsExist(uid:String):Boolean = {

    val k = "relation:" + service + ":" + uid
    client.exists(k)
    
  }
   
  def rulesExist(uid:String):Boolean = {

    val k = "rule:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def relations(uid:String):String = {

    val k = "relation:" + service + ":" + uid
    val relations = client.zrange(k, 0, -1)

    if (relations.size() == 0) {
      Serializer.serializeMultiRelations(new MultiRelations(List.empty[WeightedRules]))
    
    } else {
      
      val last = relations.toList.last
      last.split(":")(1)
      
    }
  
  }
  
  def rules(uid:String):String = {

    val k = "rule:" + service + ":" + uid
    val rules = client.zrange(k, 0, -1)

    if (rules.size() == 0) {
      Serializer.serializeRules(new Rules(List.empty[Rule]))
    
    } else {
      
      val last = rules.toList.last
      last.split(":")(1)
      
    }
  
  }

  /**
   * Retrieve those rules, where the antecedents match
   * the provided ones
   */
  def rulesByAntecedent(uid:String, antecedent:List[Int]):String = {
  
    /* Restrict to those rules, that match the antecedents */
    val items = rulesAsList(uid).filter(rule => isEqual(rule.antecedent,antecedent))
    Serializer.serializeRules(new Rules(items))
    
  } 
  /**
   * Retrieve those rules, where the consequents match
   * the provided ones
   */
  def rulesByConsequent(uid:String, consequent:List[Int]):String = {
  
    /* Restrict to those rules, that match the consequents */
    val items = rulesAsList(uid).filter(rule => isEqual(rule.consequent,consequent))
    Serializer.serializeRules(new Rules(items))

  } 
  
  private def rulesAsList(uid:String):List[Rule] = {

    val k = "rule:" + service + ":" + uid
    val rules = client.zrange(k, 0, -1)

    if (rules.size() == 0) {
      List.empty[Rule]
    
    } else {
      
      val last = rules.toList.last
      Serializer.deserializeRules(last.split(":")(1)).items
      
    }
  
  }
  
  private def isEqual(itemset1:List[Int],itemset2:List[Int]):Boolean = {
    
    val intersect = itemset1.intersect(itemset2)
    intersect.size == itemset1.size
    
  }

}