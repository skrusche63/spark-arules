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
import de.kp.spark.arules.util.JobCache

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class ARulesActor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  private val algorithmSupport = Array(ARulesAlgorithms.TOPK,ARulesAlgorithms.TOPKNR)
  
  def receive = {

    case req:String => {
      
      val origin = sender    
      /* Deserialize mining request */
      val deser = ARulesModel.deserializeRequest(req)
      
      val (uid,task,algorithm) = (deser.uid,deser.task,deser.algorithm.getOrElse(null))
      task match {
        
        case "start" => {
          /*
           * Job MUST not exist AND algorithm MUST be known,
           * then register and start job
           */          
          val resp = if (JobCache.exists(uid)) {            
            Future {
              val message = ARulesMessages.TASK_ALREADY_STARTED(uid)
              new ARulesResponse(uid, Some(message), ARulesStatus.FAILURE)            
            }
            
          } else {            
            if (algorithm == null) {   
              Future {
                val message = ARulesMessages.NO_ALGORITHM_PROVIDED(uid)
                new ARulesResponse(uid, Some(message), ARulesStatus.FAILURE)
              }
              
            } else {              
              if (algorithmSupport.contains(algorithm)) {
                
                val source = deser.source.getOrElse(null)
                if (source == null) {
                  Future {
                    val message = ARulesMessages.NO_SOURCE_PROVIDED(uid)
                    new ARulesResponse(uid, Some(message), ARulesStatus.FAILURE)
                  }
                  
                } else {
                
                  /* Build job configuration */
                  val jobConf = new JobConf()
                
                  jobConf.set("uid",uid)
                  jobConf.set("algorithm",algorithm)

                  deser.k match {
                    case None => {}
                    case Some(k) => jobConf.set("k",k)
                  }
                
                  deser.minconf match {
                    case None => {}
                    case Some(minconf) => jobConf.set("minconf",minconf)
                  }
                
                  deser.delta match {
                    case None => {}
                    case Some(delta) => jobConf.set("delta",delta)
                  }
                
                  startJob(jobConf,source).mapTo[ARulesResponse]
                 
                }
                
              } else {              
                Future {
                  val message = ARulesMessages.ALGORITHM_IS_UNKNOWN(uid,algorithm)
                  new ARulesResponse(uid, Some(message), ARulesStatus.FAILURE)
                }
                
              }
              
            }
            
          }

          resp.onSuccess {
            case result => origin ! ARulesModel.serializeResponse(result)
          }

          resp.onFailure {
            case message => {
              
              val response = new ARulesResponse(uid,Some(message.toString),ARulesStatus.FAILURE)
              origin ! ARulesModel.serializeResponse(response)	      
            
            }
	  
          }
         
        }
        
        case "stop" => {
          /*
           * Jon MUST exist then stop job
           */
          val resp = if (JobCache.exists(uid) == false) {
            val message = ARulesMessages.TASK_DOES_NOT_EXIST(uid)
            new ARulesResponse(uid, Some(message), ARulesStatus.FAILURE)
            
          } else {            
            stopJob(uid)
            
          }
           
          origin ! ARulesModel.serializeResponse(resp)
           
        }
        
        case "status" => {
          /*
           * Job MUST exist the return actual status
           */
          val resp = if (JobCache.exists(uid) == false) {           
            val message = ARulesMessages.TASK_DOES_NOT_EXIST(uid)
            new ARulesResponse(uid, Some(message), ARulesStatus.FAILURE)
            
          } else {            
            val status = JobCache.status(uid)
            new ARulesResponse(uid, None, status)
            
          }
           
          origin ! ARulesModel.serializeResponse(resp)
           
        }
        
        case _ => {
          
          val message = ARulesMessages.TASK_IS_UNKNOWN(uid,task)
          val resp = new ARulesResponse(deser.uid, Some(message), ARulesStatus.FAILURE)
           
          origin ! ARulesModel.serializeResponse(resp)
           
        }
        
      }
      
    }
    
    case _ => {}
  
  }
  
  private def startJob(jobConf:JobConf,source:ARulesSource):Future[Any] = {

    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second

    val path = source.path.getOrElse(null)
    if (path == null) {
        
      val nodes = source.nodes.getOrElse(null)
      val port  = source.port.getOrElse(null)
        
      val resource = source.resource.getOrElse(null)
      val query = source.query.getOrElse(null)
        
      val req = new ElasticRequest(nodes,port,resource,query)

      val algorithm = jobConf.get("algorithm").get.asInstanceOf[String]
      val actor = algorithmToActor(algorithm,jobConf)
      
      ask(actor, req)
        
    } else {
    
      val req = new FileRequest(path)

      val algorithm = jobConf.get("algorithm").get.asInstanceOf[String]
      val actor = algorithmToActor(algorithm,jobConf)

      ask(actor, req)
        
    }
  
  }
  
  private def algorithmToActor(algorithm:String,jobConf:JobConf):ActorRef = {

    val actor = if (algorithm == ARulesAlgorithms.TOPK) {      
      context.actorOf(Props(new TopKActor(jobConf)))      
      } else {
       context.actorOf(Props(new TopKNRActor(jobConf)))
      }
    
    actor
  
  }
  
  private def stopJob(uid:String):ARulesResponse = {
    null
  }
  
}