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
import de.kp.spark.core.io.ElasticWriter

import de.kp.spark.arules.model._

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class RuleTracker extends BaseActor {
  
  def receive = {
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
    case req:ServiceRequest => {

      val uid = req.data("uid")
      
      val data = Map("uid" -> uid, "message" -> Messages.TRACKED_ITEM_RECEIVED(uid))
      val response = new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)	
      
      val origin = sender
      origin ! response

      try {

        val index   = req.data("index")
        val mapping = req.data("type")
    
        val writer = new ElasticWriter()
        
        val readyToWrite = writer.open(index,mapping)
        if (readyToWrite == false) {
      
          writer.close()
      
          val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
          throw new Exception(msg)
      
        } else {
          
          /*
           * Set status to indicate that the data tracking has started
           */
          cache.addStatus(req,ResponseStatus.TRACKING_STARTED)
      
          /*
           * Data preparation comprises the extraction of all common 
           * fields, i.e. timestamp, site, user and group. The 'item' 
           * field may specify a list of purchase items and has to be 
           * processed differently.
           */
          val source = prepare(req.data)
          /*
           * The 'item' field specifies a comma-separated list
           * of item (e.g.) product identifiers. Note, that every
           * item is actually indexed individually. This is due to
           * synergy effects with other data sources
           */
          val items = req.data(Names.ITEM_FIELD).split(",")
          for  (item <- items) {
            
            /*
             * Set or overwrite the 'item' field in the respective source
             */
            source.put(Names.ITEM_FIELD, item)
            
            /*
             * Writing this source to the respective index throws an
             * exception in case of an error; note, that the writer is
             * automatically closed 
             */
            writer.write(index, mapping, source)
            
          }
          
          /*
           * Set status to indicate that the respective data have
           * been tracked sucessfully
           */
          cache.addStatus(req,ResponseStatus.TRACKING_FINISHED)
          
        }
      
      } catch {
        
        case e:Exception => {
          log.error(e, e.getMessage())
        }
      
      } finally {
        
        context.stop(self)

      }
    }
    
  }
  
  private def prepare(params:Map[String,String]):java.util.Map[String,Object] = {
    
    val source = HashMap.empty[String,String]
    
    source += Names.SITE_FIELD -> params(Names.SITE_FIELD)
    source += Names.USER_FIELD -> params(Names.USER_FIELD)
      
    source += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD) 
    source += Names.GROUP_FIELD -> params(Names.GROUP_FIELD)
 
    source
    
  }
 
}