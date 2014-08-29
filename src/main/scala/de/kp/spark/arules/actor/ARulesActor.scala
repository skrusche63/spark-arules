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

import de.kp.spark.arules.{ARulesAlgorithms,ARulesMessages,ARulesModel,ARulesRequest,ARulesResponse,ARulesStatus}
import de.kp.spark.arules.util.JobCache

import java.util.Date

class ARulesActor extends Actor with ActorLogging {
  
  private val algorithmSupport = Array(ARulesAlgorithms.TOPK,ARulesAlgorithms.TOPKNR)
  
  def receive = {

    case req:String => {
      
      val origin = sender    
      /* Deserialize mining request */
      val deser = ARulesModel.deserializeRequest(req)
            
      val now = new Date()
      val time = now.getTime()
      
      val (uid,task,algorithm) = (deser.uid,deser.task,deser.algorithm.getOrElse(null))
      task match {
        
        case "start" => {
          /*
           * Job MUST not exist AND algorithm MUST be known,
           * then register and start job
           */          
          val resp = if (JobCache.exists(uid)) {
            val message = ARulesMessages.TASK_ALREADY_STARTED(uid)
            new ARulesResponse(uid, Some(message), ARulesStatus.FAILURE)            

          } else {            
            if (algorithm == null) {            
              val message = ARulesMessages.NO_ALGORITHM_PROVIDED(uid)
              new ARulesResponse(uid, Some(message), ARulesStatus.FAILURE)
              
            } else {              
              if (algorithmSupport.contains(algorithm)) {
                startJob(uid,time,algorithm)
                
              } else {              
                val message = ARulesMessages.ALGORITHM_IS_UNKNOWN(uid,algorithm)
                new ARulesResponse(uid, Some(message), ARulesStatus.FAILURE)
                
              }
              
            }
            
          }
            
          origin ! resp
          
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
            
          origin ! resp
          
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
            
          origin ! resp
          
        }
        
        case _ => {
          
          val message = ARulesMessages.TASK_IS_UNKNOWN(uid,task)
          val resp = new ARulesResponse(deser.uid, Some(message), ARulesStatus.FAILURE)
          
          origin ! resp
          
        }
        
      }
      
    }
    
    case _ => {}
  
  }
  
  private def startJob(uid:String,time:Long,algorithm:String):ARulesResponse = {

    val status = ARulesStatus.STARTED
    JobCache.add(uid,time,status)
    null
  }
  
  private def stopJob(uid:String):ARulesResponse = {
    null
  }
}