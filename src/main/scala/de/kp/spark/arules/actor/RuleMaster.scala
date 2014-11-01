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

import org.apache.spark.SparkContext
import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class RuleMaster(@transient val sc:SparkContext) extends Actor with ActorLogging {
  
  val (duration,retries,time) = Configuration.actor   

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }
  
  def receive = {
    
    case req:String => {
      
      implicit val ec = context.dispatcher
      implicit val timeout:Timeout = DurationInt(time).second
	  	    
	  val origin = sender

	  val deser = Serializer.deserializeRequest(req)
	  val response = deser.task.split(":")(0) match {
        /*
         * Retrieve all the relations or rules discovered by a 
         * previous mining task; relevant is the 'uid' of the 
         * mining task to get the respective data
         */
        case "get" => ask(actor("questor"),deser).mapTo[ServiceResponse]
        /*
         * Request to register field specification
         */
        case "register"  => ask(actor("registrar"),deser).mapTo[ServiceResponse]
        /*
         * Request the actual status of an association rule
         * mining task; note, that get requests should only
         * be invoked after having retrieved a FINISHED status
         */
        case "status" => ask(actor("miner"),deser).mapTo[ServiceResponse]
        /*
         * Start the association rule mining
         */
        case "train"  => ask(actor("miner"),deser).mapTo[ServiceResponse]
        /*
         * Track item for later association rule mining
         */
        case "track"  => ask(actor("tracker"),deser).mapTo[ServiceResponse]
       
        case _ => {

          Future {     
            failure(deser,Messages.TASK_IS_UNKNOWN(deser.data("uid"),deser.task))
          } 
        
        }
      
      }
      response.onSuccess {
        case result => origin ! Serializer.serializeResponse(result)
      }
      response.onFailure {
        case result => origin ! failure(deser,Messages.GENERAL_ERROR(deser.data("uid")))	      
	  }
      
    }
  
    case _ => {

      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! Serializer.serializeResponse(failure(null,msg))

    }
    
  }

  private def actor(worker:String):ActorRef = {
    
    worker match {
  
      case "miner" => context.actorOf(Props(new RuleMiner(sc)))
        
      case "questor" => context.actorOf(Props(new RuleQuestor()))
        
      case "registrar" => context.actorOf(Props(new RuleRegistrar()))
   
      case "tracker" => context.actorOf(Props(new RuleTracker()))
      
      case _ => null
      
    }
  
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    if (req == null) {
      val data = Map("message" -> message)
      new ServiceResponse("","",data,ARulesStatus.FAILURE)	
      
    } else {
      val data = Map("uid" -> req.data("uid"), "message" -> message)
      new ServiceResponse(req.service,req.task,data,ARulesStatus.FAILURE)	
    
    }
    
  }

}