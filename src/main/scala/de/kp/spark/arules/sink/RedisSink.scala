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

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model._

import scala.collection.JavaConversions._

class RedisSink(host:String,port:Int) extends RedisDB(host,port){

  val service = "arules"

  def addUserRules(req:ServiceRequest, relations:MultiUserRules) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "multi:user:rule:" + service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeMultiUserRules(relations)
    
    getClient.zadd(k,timestamp,v)
    
  }
 
  def userRulesExist(uid:String):Boolean = {

    val k = "multi:user:rule:" + service + ":" + uid
    getClient.exists(k)
    
  }
    
  def rulesByAllUsers(uid:String):String = {

    val k = "multi:user:rule:" + service + ":" + uid
    val rules = getClient.zrange(k, 0, -1)

    if (rules.size() == 0) {
      Serializer.serializeMultiUserRules(new MultiUserRules(List.empty[UserRules]))
    
    } else {
      
      val last = rules.toList.last
      last.split(":")(1)
      
    }
  
  }
  
  def rulesByUsers(uid:String,site:String,users:List[String]):String = {
                
    val rules = Serializer.deserializeMultiUserRules(rulesByAllUsers(uid)).items
            
    /*
     * The users are used as a filter for the respective rules;
     * note, that a user entry has the following format: site|user
     * 
     * This implies that the respective data from rules must be piped
     */
    val filteredRules = rules.filter(entry => site == entry.site && users.contains(entry.user))
    Serializer.serializeMultiUserRules(new MultiUserRules(filteredRules))

  }

}