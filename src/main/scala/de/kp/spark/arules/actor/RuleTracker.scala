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
import de.kp.spark.core.actor.BaseTracker

import de.kp.spark.core.model._
import de.kp.spark.arules.Configuration

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class RuleTracker extends BaseTracker(Configuration) {
 
  /*
   * Example request data:
   * 
   * "uid": "123456"
   * 
   * "index": "orders"
   * "type" : "products"
   * 
   * "site"    : "site-1"
   * "user"    : "user-1"
   * "timestamp: "1234567890"
   * "group"   : "group-1"
   * "item"    : "1,2,3,4,5,6,7"
   * 
   */
  
  override def prepareEvent(params:Map[String,String]):java.util.Map[String,Object] = null
  
  override def prepareItem(params:Map[String,String]):java.util.Map[String,Object] = {
    
    val source = HashMap.empty[String,String]
    
    source += Names.SITE_FIELD -> params(Names.SITE_FIELD)
    source += Names.USER_FIELD -> params(Names.USER_FIELD)
      
    source += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD) 
    source += Names.GROUP_FIELD -> params(Names.GROUP_FIELD)
 
    source
    
  }
 
}