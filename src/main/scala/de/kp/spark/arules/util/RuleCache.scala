package de.kp.spark.arules.util
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

import de.kp.spark.arules.{Configuration,Rule}
import java.util.Date

object RuleCache {
  
  private val maxentries = Configuration.cache  
  private val cache = new LRUCache[(String,Long),List[Rule]](maxentries)

  def add(uid:String,rules:List[Rule]) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = (uid,timestamp)
    val v = rules
    
    cache.put(k,v)
    
  }
  
  def exists(uid:String):Boolean = {
    
    val keys = cache.keys().filter(key => key._1 == uid)
    (keys.size > 0)
    
  }
  
  def rules(uid:String):List[Rule] = {
    
    val keys = cache.keys().filter(key => key._1 == uid)
    if (keys.size == 0) {    
      null
      
    } else {
      
      val last = keys.sortBy(_._2).last
      cache.get(last) match {
        
        case None => null
        case Some(rules) => rules
      
      }
      
    }
  
  }

  /**
   * Retrieve those rules, where the antecedents match
   * the provided ones
   */
  def rulesByAntecedent(uid:String, antecedent:List[Int]):List[Rule] = {
  
    /* Restrict to those rules, that match the antecedents */
    rules(uid).filter(rule => isEqual(rule.antecedent,antecedent))

  } 
  /**
   * Retrieve those rules, where the consequents match
   * the provided ones
   */
  def rulesByConsequent(uid:String, consequent:List[Int]):List[Rule] = {
  
    /* Restrict to those rules, that match the consequents */
    rules(uid).filter(rule => isEqual(rule.consequent,consequent))

  } 
  
  private def isEqual(itemset1:List[Int],itemset2:List[Int]):Boolean = {
    
    val intersect = itemset1.intersect(itemset2)
    intersect.size == itemset1.size
    
  }
}