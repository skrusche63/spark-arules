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

import akka.actor.{Actor,ActorLogging,ActorRef,Props}
import akka.pattern.ask
import akka.util.Timeout
import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model._
import de.kp.spark.arules.util.{JobCache,RuleCache}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import org.apache.pig.builtin.TOMAP

class RuleMiner extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  private val algorithms = Array(ARulesAlgorithms.TOPK,ARulesAlgorithms.TOPKNR)
  private val sources = Array(ARulesSources.FILE,ARulesSources.ELASTIC,ARulesSources.JDBC)
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task match {
        
        case "train" => {
          
          val response = validate(req.data) match {
            
            case None => train(req).mapTo[ServiceResponse]            
            case Some(message) => Future {failure(req,message)}
            
          }

          response.onSuccess {
            case result => origin ! ARulesModel.serializeResponse(result)
          }

          response.onFailure {
            case throwable => {             
              val resp = failure(req,throwable.toString)
              origin ! ARulesModel.serializeResponse(resp)	                  
            }	  
          }
         
        }
       
        case "status" => {
          
          /*
           * Job MUST exist the return actual status
           */
          val resp = if (JobCache.exists(uid) == false) {           
            failure(req,ARulesMessages.TASK_DOES_NOT_EXIST(uid))
            
          } else {            
            status(req)
            
          }
           
          origin ! ARulesModel.serializeResponse(resp)
           
        }
        
        case _ => {
          
          val msg = ARulesMessages.TASK_IS_UNKNOWN(uid,req.task)
          origin ! ARulesModel.serializeResponse(failure(req,msg))
           
        }
        
      }
      
    }
    
    case _ => {}
  
  }
  
  private def train(req:ServiceRequest):Future[Any] = {

    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    /*
     * Retrieve appropriate actor from initial request parameters
     */
    val actor = toActor(req)
    /*
     * Retrieve appropriate source request from paramaters
     */
    val params = req.data
    
    val source = params("source")
    if (source == ARulesSources.FILE) {
      /*
       * Retrieve transaction database from file system; the respective
       * path is provided through configuration paramaters
       */
      val request = new FileRequest()      
      ask(actor, request)
      
    } else if (source == ARulesSources.ELASTIC) {
      /*
       * Retrieve transaction database from an Elasticsearch search index;
       * the respective access parameters are provided through configuration
       * parameters
       */
      val request = new ElasticRequest()      
      ask(actor, request)
      
    } else {
      /*
       * Retrieve transaction database from JDBC database
       */
      val site  = params("site").toInt
      val query = params("query")
      
      val request = new JdbcRequest(site,query)      
      ask(actor, request)
      
    }
    
  }

  private def status(req:ServiceRequest):ServiceResponse = {
    
    val uid = req.data("uid")
    val data = Map("uid" -> uid)
                
    new ServiceResponse(req.service,req.task,data,JobCache.status(uid))	

  }

  private def validate(params:Map[String,String]):Option[String] = {

    val uid = params("uid")
    
    if (JobCache.exists(uid)) {            
      return Some(ARulesMessages.TASK_ALREADY_STARTED(uid))    
    }

    params.get("algorithm") match {
        
      case None => {
        return Some(ARulesMessages.NO_ALGORITHM_PROVIDED(uid))              
      }
        
      case Some(algorithm) => {
        if (algorithms.contains(algorithm) == false) {
          return Some(ARulesMessages.ALGORITHM_IS_UNKNOWN(uid,algorithm))    
        }
          
      }
    
    }  
    
    params.get("source") match {
        
      case None => {
        return Some(ARulesMessages.NO_SOURCE_PROVIDED(uid))          
      }
        
      case Some(source) => {
        if (sources.contains(source) == false) {
          return Some(ARulesMessages.SOURCE_IS_UNKNOWN(uid,source))    
        }          
      }
        
    }

    None
    
  }

  private def toActor(req:ServiceRequest):ActorRef = {

    val algorithm = req.data("algorithm")
    val actor = if (algorithm == ARulesAlgorithms.TOPK) {      
      context.actorOf(Props(new TopKActor(req)))      
    } else {
     context.actorOf(Props(new TopKNRActor(req)))
    }
    
    actor
  
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,ARulesStatus.FAILURE)	
    
  }
  
}