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

class ARulesMiner extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  private val algorithmSupport = Array(ARulesAlgorithms.TOPK,ARulesAlgorithms.TOPKNR)
  
  def receive = {

    case req:ARulesRequest => {
      
      val origin = sender    
      
      val (uid,task) = (req.uid,req.task)
      task match {
        
        case "start" => {
          
          val algorithm  = req.algorithm.getOrElse(null)
          val parameters = req.parameters.getOrElse(null)
          
          val source = req.source.getOrElse(null)
          
          val response = validateStart(uid,algorithm,parameters,source) match {
            
            case None => {
              /* Build job configuration */
              val jobConf = new JobConf()
                
              jobConf.set("uid",uid)
              jobConf.set("algorithm",algorithm)

              jobConf.set("k",parameters.k)
              jobConf.set("minconf",parameters.minconf)
               
              parameters.delta match {
                case None => {}
                case Some(delta) => jobConf.set("delta",delta)
              }
              /* Start job */
              startJob(jobConf,source).mapTo[ARulesResponse]
              
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
  
  private def startJob(jobConf:JobConf,source:ARulesSource):Future[Any] = {

    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second

    val algorithm = jobConf.get("algorithm").get.asInstanceOf[String]
    val actor = algorithmToActor(algorithm,jobConf)

    val path = source.path.getOrElse(null)
    if (path == null) {

      val req = new ElasticRequest()      
      ask(actor, req)
        
    } else {
    
      val req = new FileRequest(path)
      ask(actor, req)
        
    }
  
  }

  private def validateStart(uid:String,algorithm:String,parameters:ARulesParameters,source:ARulesSource):Option[String] = {

    if (JobCache.exists(uid)) {            
      val message = ARulesMessages.TASK_ALREADY_STARTED(uid)
      return Some(message)
    
    }
            
    if (algorithm == null) {   
      val message = ARulesMessages.NO_ALGORITHM_PROVIDED(uid)
      return Some(message)
    
    }
              
    if (algorithmSupport.contains(algorithm) == false) {
      val message = ARulesMessages.ALGORITHM_IS_UNKNOWN(uid,algorithm)
      return Some(message)
    
    }
    
    if (parameters == null) {
      val message = ARulesMessages.NO_PARAMETERS_PROVIDED(uid)
      return Some(message)
      
    }
    
    if (source == null) {
      val message = ARulesMessages.NO_SOURCE_PROVIDED(uid)
      return Some(message)
 
    }

    None
    
  }

  private def algorithmToActor(algorithm:String,jobConf:JobConf):ActorRef = {

    val actor = if (algorithm == ARulesAlgorithms.TOPK) {      
      context.actorOf(Props(new TopKActor(jobConf)))      
      } else {
       context.actorOf(Props(new TopKNRActor(jobConf)))
      }
    
    actor
  
  }
  
}