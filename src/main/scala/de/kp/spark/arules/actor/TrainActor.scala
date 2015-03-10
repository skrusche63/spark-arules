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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.redis.RedisDB
import de.kp.spark.arules.RequestContext

import de.kp.spark.arules.model._
import de.kp.spark.arules.sink._

/**
 * MLActor comprises common functionality for the algorithm specific
 * actors, TopKActor and TopKNRActor
 */
abstract class TrainActor(@transient ctx:RequestContext) extends BaseActor {

  private val (host,port) = ctx.config.redis
  private val redis = new RedisDB(host,port.toInt)
  
  private val parquet = new ParquetSink(ctx)
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender
      val missing = try {
        
        validate(req)
        false
      
      } catch {
        case e:Exception => true
        
      }

      origin ! response(req, missing)

      if (missing == false) {
 
        try {

          /* Update cache */
          cache.addStatus(req,ResponseStatus.MINING_STARTED)
          
          train(req)
          
          /* Update cache */
          cache.addStatus(req,ResponseStatus.MINING_FINISHED)
 
        } catch {
          case e:Exception => cache.addStatus(req,ResponseStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("unknown request.")
      context.stop(self)
      
    }
    
  }
  
  protected def validate(req:ServiceRequest)
  
  protected def train(req:ServiceRequest)

  /**
   * Rules are actually registered in an internal Redis instance as well
   * as in an Elasticsearch index or a Jdbc database
   */
  protected def saveRules(req:ServiceRequest,rules:Rules) {
    
    /*
     * Discovered rules are always registered in an internal Redis instance
     * for fast rule access, and also as a Parquet file for sharing with other
     * applications.
     * 
     * In addition, additional data sinks may be used; this depends on whether 
     * a cetain sink has been specified with the request
     */
    redis.addRules(req,rules)
    
    if (req.data.contains(Names.REQ_SINK) == false) return
    
    val sink = req.data(Names.REQ_SINK)
    if (Sinks.isSink(sink) == false) return
    
    sink match {
      
      case Sinks.ELASTIC => {
    
        val elastic = new ElasticSink()
        elastic.addRules(req,rules)
        
      }
      
      case Sinks.JDBC => {
            
        val jdbc = new JdbcSink()
        jdbc.addRules(req,rules)
 
      }
      
      case _ => {/* do nothing */}
      
    }
    
  }
  
  protected def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data(Names.REQ_UID)
    
    if (missing == true) {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
  
    } else {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MINING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.MINING_STARTED)	
  
    }

  }

}