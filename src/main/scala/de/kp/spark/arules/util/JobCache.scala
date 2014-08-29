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

import de.kp.spark.arules.Configuration

object JobCache {
  
  private val maxentries = Configuration.cache  
  private val cache = new LRUCache[(String,Long),String](maxentries)

  def add(uid:String,timestamp:Long,status:String) {
   
    val k = (uid,timestamp)
    val v = status
    
    cache.put(k,v)
    
  }
  
  def exists(uid:String):Boolean = {
    
    val keys = cache.keys().filter(key => key._1 == uid)
    (keys.size > 0)
    
  }
  
  /**
   * Get timestamp when job with 'uid' started
   */
  def starttime(uid:String):Long = {
    
    val keys = cache.keys().filter(key => key._1 == uid)
    if (keys.size == 0) {
      0
    
    } else {
      
      val first = keys.sortBy(_._2).head
      first._2
      
    }
    
  }
  
  def status(uid:String):String = {
    
    val keys = cache.keys().filter(key => key._1 == uid)
    if (keys.size == 0) {    
      null
      
    } else {
      
      val last = keys.sortBy(_._2).last
      cache.get(last) match {
        
        case None => null
        case Some(status) => status
      
      }
      
    }
  }

}