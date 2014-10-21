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

import akka.actor.{Actor,ActorLogging}

import de.kp.spark.arules.model._

import de.kp.spark.arules.io.ElasticWriter
import de.kp.spark.arules.io.{ElasticBuilderFactory => EBF}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class RuleTracker extends Actor with ActorLogging {
  
  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")
      
      val data = Map("uid" -> uid, "message" -> Messages.TRACKED_ITEM_RECEIVED(uid))
      val response = new ServiceResponse(req.service,req.task,data,ARulesStatus.SUCCESS)	
      
      val origin = sender
      origin ! Serializer.serializeResponse(response)

      try {
        /*
         * Elasticsearch is used as a source and also as a sink; this implies
         * that the respective index and mapping must be distinguished; the source
         * index and mapping used here is the same as for ElasticSource
         */
        val index   = req.data("source.index")
        val mapping = req.data("source.type")
    
        val builder = EBF.getBuilder("rule",mapping)
        val writer = new ElasticWriter()
    
        /* Prepare index and mapping for write */
        val readyToWrite = writer.open(index,mapping,builder)
        if (readyToWrite == false) {
      
          writer.close()
      
          val msg = String.format("""Opening index '%s' and maping '%s' for write failed.""",index,mapping)
          throw new Exception(msg)
      
        } else {
      
          /* Prepare data */
          val source = prepare(req.data)
          /*
           * Writing this source to the respective index throws an
           * exception in case of an error; note, that the writer is
           * automatically closed 
           */
          writer.write(index, mapping, source)
        
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
    
    source += EBF.SITE_FIELD -> params(EBF.SITE_FIELD)
    source += EBF.USER_FIELD -> params(EBF.USER_FIELD)
      
    source += EBF.TIMESTAMP_FIELD -> params(EBF.TIMESTAMP_FIELD)
 
    source += EBF.GROUP_FIELD -> params(EBF.GROUP_FIELD)
    source += EBF.ITEM_FIELD -> params(EBF.ITEM_FIELD)

    source
    
  }
 
}