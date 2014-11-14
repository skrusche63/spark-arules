package de.kp.spark.arules.redis
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

import de.kp.spark.arules.model._

import java.util.Date
import scala.collection.JavaConversions._

object RedisCache {

  val client  = RedisClient()
  /**
   * The Redis instance is used to register the field description
   * that has to be used for a certain mining task to map data source
   * fields onto the internal data format
   */
  def addFields(req:ServiceRequest,fields:Fields) {
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "fields:association:" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeFields(fields)
    
    client.zadd(k,timestamp,v)
    
  }

  /**
   * The Redis instance is used to register all requests that have been made
   * in the sense of a log server; order to be able to build timelines and
   * inform users about the processing history
   */
  def addRequest(req:ServiceRequest) {
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "request:association"
    val v = "" + timestamp + ":" + Serializer.serializeRequest(req)
    
    client.lpush(k,v)
    
  }
  /**
   * Add a job description of a certain interim step of a certain 
   * data mining task to the Redis instance
   */
  def addStatus(req:ServiceRequest,status:String) {
   
    val (uid,task) = (req.data("uid"),req.task)
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "job:association:" + uid
    val v = "" + timestamp + ":" + Serializer.serializeJob(JobDesc(req.service,task,status))
    
    client.zadd(k,timestamp,v)
    
  }
  /** 
   *  Determine whether a field description has been registered
   *  for a certain data mining task
   */
  def fieldsExist(uid:String):Boolean = {

    val k = "fields:association:" + uid
    client.exists(k)
    
  }
  
  def taskExists(uid:String):Boolean = {

    val k = "job:association:" + uid
    client.exists(k)
    
  }
  
  /**
   * Retrieve the field descriptions from the 
   * Redis instance with respect to a certain 
   * unique task identifier
   */
  def fields(uid:String):Fields = {

    val k = "fields:association:" + uid
    val metas = client.zrange(k, 0, -1)

    if (metas.size() == 0) {
      new Fields(List.empty[Field])
    
    } else {
      
      val fields = metas.toList.last
      Serializer.deserializeFields(fields)
      
    }

  }
  
  /**
   * Retrieve the total number of requests registered
   * with the Redis instance
   */
  def requestsTotal():Long = {

    val k = "request:association"
    if (client.exists(k)) client.llen(k) else 0
    
  }
  /**
   * Retrieve a slice of requests from the Redis instance
   */
  def requests(start:Long,end:Long):List[(Long,ServiceRequest)] = {
    
    val k = "request:association"
    val requests = client.lrange(k, start, end)
    
    requests.map(request => {
      
      val Array(ts,req) = request.split(":")
      (ts.toLong,Serializer.deserializeRequest(req))
      
    }).toList
    
  }
  /**
   * Retrieve the status of the latest job description 
   * of a certain mining task
   */
  def status(uid:String):String = {

    val k = "job:association:" + uid
    val data = client.zrange(k, 0, -1)

    if (data.size() == 0) {
      null
    
    } else {
      
      /* Format: timestamp:jobdesc */
      val last = data.toList.last
      val Array(timestamp,jobdesc) = last.split(":")
      
      val job = Serializer.deserializeJob(jobdesc)
      job.status
      
    }

  }
  /**
   * Retrieve all job descriptions of a certain mining task 
   * in the respective time order
   */
  def statuses(uid:String):List[(Long,JobDesc)] = {
    
    val k = "job:association:" + uid
    val data = client.zrange(k, 0, -1)

    if (data.size() == 0) {
      null
    
    } else {
      
      data.map(record => {
        
        val Array(timestamp,jobdesc) = record.split(":")
        (timestamp.toLong,Serializer.deserializeJob(jobdesc))
        
      }).toList
      
    }
    
  }
}