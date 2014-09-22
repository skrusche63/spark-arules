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

class ARulesMiner extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  private val algorithms = Array(ARulesAlgorithms.TOPK,ARulesAlgorithms.TOPKNR)
  private val sources = Array(ARulesSources.FILE,ARulesSources.ELASTIC,ARulesSources.JDBC)
  
  def receive = {

    case req:ARulesRequest => {
      
      val origin = sender    
      
      val (uid,task) = (req.uid,req.task)
      task match {
        
        case "start" => {
          
          val parameters = req.parameters.getOrElse(null)          
          val response = validateStart(uid,parameters) match {
            
            case None => {
              /* Build job configuration */
              val jobConf = new JobConf()
              val params = parameters.map(p => (p.name, p.valu)).toMap
              
              /* Start job */
              startJob(uid,params).mapTo[ARulesResponse]
              
            }
            
            case Some(message) => {
              Future {new ARulesResponse(uid,Some(message),None,None,ARulesStatus.FAILURE)} 
              
            }
            
          }

          response.onSuccess {
            case result => origin ! ARulesModel.serializeResponse(result)
          }

          response.onFailure {
            case message => {             
              val resp = new ARulesResponse(uid,Some(message.toString),None,None,ARulesStatus.FAILURE)
              origin ! ARulesModel.serializeResponse(resp)	                  
            }	  
          }
         
        }
       
        case "status" => {
          /*
           * Job MUST exist the return actual status
           */
          val resp = if (JobCache.exists(uid) == false) {           
            val message = ARulesMessages.TASK_DOES_NOT_EXIST(uid)
            new ARulesResponse(uid,Some(message),None,None,ARulesStatus.FAILURE)
            
          } else {            
            val status = JobCache.status(uid)
            new ARulesResponse(uid,None,None,None,status)
            
          }
           
          origin ! ARulesModel.serializeResponse(resp)
           
        }
        
        case _ => {
          
          val message = ARulesMessages.TASK_IS_UNKNOWN(uid,task)
          val resp = new ARulesResponse(uid,Some(message),None,None,ARulesStatus.FAILURE)
           
          origin ! ARulesModel.serializeResponse(resp)
           
        }
        
      }
      
    }
    
    case _ => {}
  
  }
  
  private def startJob(uid:String,params:Map[String,String]):Future[Any] = {

    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    /*
     * Retrieve appropriate actor from initial request parameters
     */
    val actor = toActor(uid,params)
    /*
     * Retrieve appropriate source request from paramaters
     */
    val source = params("source")
    if (source == ARulesSources.FILE) {
      /*
       * Retrieve transaction database from file system; the respective
       * path is provided through configuration paramaters
       */
      val req = new FileRequest()      
      ask(actor, req)
      
    } else if (source == ARulesSources.ELASTIC) {
      /*
       * Retrieve transaction database from an Elasticsearch search index;
       * the respective access parameters are provided through configuration
       * parameters
       */
      val req = new ElasticRequest()      
      ask(actor, req)
      
    } else {
      /*
       * Retrieve transaction database from JDBC database
       */
      val site  = params("site").toInt
      val query = params("query")
      
      val req = new JdbcRequest(site,query)      
      ask(actor, req)
      
    }
    
  }

  private def validateStart(uid:String,parameters:List[ARulesParameter]):Option[String] = {

    if (JobCache.exists(uid)) {            
      val message = ARulesMessages.TASK_ALREADY_STARTED(uid)
      return Some(message)
    
    }
    
    if (parameters == null) {
      val message = ARulesMessages.NO_PARAMETERS_PROVIDED(uid)
      return Some(message)
      
    }

    /*
     * Distinguish between different parameters
     */
    val params = parameters.map(p => (p.name,p.valu)).toMap
    try {
      /*
       * Validate 'algorithm'
       */
      params.get("algorithm") match {
        
        case None => {
          val message = ARulesMessages.NO_ALGORITHM_PROVIDED(uid)
          return Some(message)              
        }
        
        case Some(algorithm) => {
          if (algorithms.contains(algorithm) == false) {
            val message = ARulesMessages.ALGORITHM_IS_UNKNOWN(uid,algorithm)
            return Some(message)    
          }
          
        }
      }
      
      params.get("source") match {
        
        case None => {
          val message = ARulesMessages.NO_SOURCE_PROVIDED(uid)
          return Some(message)          
        }
        
        case Some(source) => {
          if (sources.contains(source) == false) {
            val message = ARulesMessages.SOURCE_IS_UNKNOWN(uid,source)
            return Some(message)    
          }
          
        }
        
      }
      
    } catch {
      case e:Exception => {/* do nothing */}
    }

    None
    
  }

  private def toActor(uid:String,params:Map[String,String]):ActorRef = {

    val conf = new JobConf()
    conf.set("uid",uid)

    params.get("k") match {
      case None => {}
      case Some(k) => conf.set("k",k)
    }
              
    params.get("minconf") match {
     case None => {}
     case Some(minconf) => conf.set("minconf",minconf)
    }
               
    params.get("delta") match {
      case None => {}
      case Some(delta) => conf.set("delta",delta)
    }
    
    val algorithm = params("algorithm")
    val actor = if (algorithm == ARulesAlgorithms.TOPK) {      
      context.actorOf(Props(new TopKActor(conf)))      
    } else {
     context.actorOf(Props(new TopKNRActor(conf)))
    }
    
    actor
  
  }
  
}