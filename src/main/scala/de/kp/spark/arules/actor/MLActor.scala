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
import de.kp.spark.arules.sink.{ElasticSink,JdbcSink,RedisSink}

/**
 * MLActor comprises common functionality for the algorithm specific
 * actors, TopKActor and TopKNRActor
 */
abstract class MLActor extends Actor with ActorLogging {
  
  protected def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ARulesStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.MINING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ARulesStatus.STARTED)	
  
    }

  }
  
  /**
   * (Multi-)Relations are actually registered in an internal Redis instance
   * only; note, that is different from rules, which are also indexed in an
   * Elasticsearch index
   */
  protected def saveRelations(req:ServiceRequest,relations:MultiRelations) {

    val sink = new RedisSink()
    sink.addRelations(req,relations)
    
  }

  /**
   * Rules are actually registered in an internal Redis instance as well
   * as in an Elasticsearch index or a Jdbc database
   */
  protected def saveRules(req:ServiceRequest,rules:Rules) {
    
    /*
     * Discovered rules are always registered in an internal Redis instance;
     * on a per request basis, additional data sinks may be used; this depends
     * on whether a cetain sink has been specified with the request
     */
    val redis = new RedisSink()
    redis.addRules(req,rules)
    
    if (req.data.contains("sink") == false) return
    
    val sink = req.data("sink")
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

}