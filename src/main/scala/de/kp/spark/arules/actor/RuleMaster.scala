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
import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}
import de.kp.spark.core.model._

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class RuleMaster(@transient val sc:SparkContext) extends BaseActor {

  val (duration,retries,time) = Configuration.actor   
	  	    
  implicit val ec = context.dispatcher
  implicit val timeout:Timeout = DurationInt(time).second

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }
  
  def receive = {
    /*
     * This request is initiated by a remote Akka service and
     * delegated from the RuleService
     */
    case req:String => {

	  val origin = sender

	  val deser = Serializer.deserializeRequest(req)
	  val response = execute(deser)
	  
      response.onSuccess {
        case result => origin ! Serializer.serializeResponse(result)
      }
      response.onFailure {
        case result => origin ! failure(deser,Messages.GENERAL_ERROR(deser.data("uid")))	      
	  }
      
    }
    /*
     * This request is initiated by the Rest API
     */
    case req:ServiceRequest => {

	  val origin = sender

	  val response = execute(req)	  
      response.onSuccess {
        case result => origin ! Serializer.serializeResponse(result)
      }
      response.onFailure {
        case result => origin ! failure(req,Messages.GENERAL_ERROR(req.data("uid")))	      
	  }
      
    }
  
    case _ => {

      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! Serializer.serializeResponse(failure(null,msg))

    }
    
  }

  private def execute(req:ServiceRequest):Future[ServiceResponse] = {
	  
    req.task.split(":")(0) match {
      /*
       * Retrieve all the relations or rules discovered by a 
       * previous mining task; relevant is the 'uid' of the 
       * mining task to get the respective data
       */
      case "get" => ask(actor("questor"),req).mapTo[ServiceResponse]
      /*
       * Request to prepare the Elasticsearch index for subsequent
       * tracking events; this is an event invoked by the admin
       * interface
       */
      case "index" => ask(actor("indexer"),req).mapTo[ServiceResponse]
      /*
       * Request to register the field specification, that determines
       * which data source fields map onto the internal format used
       */
      case "register" => ask(actor("registrar"),req).mapTo[ServiceResponse]
      /*
       * Request the actual status of an association rule
       * mining task; note, that get requests should only
       * be invoked after having retrieved a FINISHED status
       */
      case "status" => ask(actor("miner"),req).mapTo[ServiceResponse]
      /*
       * Start association rule mining
       */
      case "train"  => ask(actor("miner"),req).mapTo[ServiceResponse]
      /*
       * Track item for later association rule mining
       */
      case "track"  => ask(actor("tracker"),req).mapTo[ServiceResponse]
       
      case _ => {

        Future {     
          failure(req,Messages.TASK_IS_UNKNOWN(req.data("uid"),req.task))
        } 
        
      }
      
    }
    
  }
  
  private def actor(worker:String):ActorRef = {
    
    worker match {
  
      case "indxer" => context.actorOf(Props(new RuleIndexer()))
  
      case "miner" => context.actorOf(Props(new RuleMiner(sc)))
        
      case "questor" => context.actorOf(Props(new RuleQuestor()))
        
      case "registrar" => context.actorOf(Props(new RuleRegistrar()))
   
      case "tracker" => context.actorOf(Props(new RuleTracker()))
      
      case _ => null
      
    }
  
  }

}