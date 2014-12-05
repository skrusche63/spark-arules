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

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
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
     * This request is initiated by the Akka API; 
     * the respective responses MUST be serialized
     */
    case msg:String => {

	  val origin = sender

	  val req = Serializer.deserializeRequest(msg)
	  val response = execute(req)
	  
      response.onSuccess {
        case result => origin ! serialize(result)
      }
      response.onFailure {
        case result => origin ! serialize(failure(req,Messages.GENERAL_ERROR(req.data(Names.REQ_UID))))	      
	  }
      
    }
    /*
     * This request is initiated by the Rest API; 
     * the respective responses MUST not be serialized
     */
    case req:ServiceRequest => {

	  val origin = sender

	  val response = execute(req)	  
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! failure(req,Messages.GENERAL_ERROR(req.data(Names.REQ_UID)))      
	  }
      
    }
  
    case _ => {

      val msg = Messages.REQUEST_IS_UNKNOWN()          
      log.error(msg)

    }
    
  }

  private def execute(req:ServiceRequest):Future[ServiceResponse] = {
	
    try {
      
      val task = req.task.split(":")(0)
      ask(actor(task),req).mapTo[ServiceResponse]
    
    } catch {
      
      case e:Exception => {
        Future {failure(req,e.getMessage)}         
      }
    
    }
     
  }
  
  private def actor(worker:String):ActorRef = {
    
    worker match {
      /*
       * Metadata management is part of the core functionality; field or metadata
       * specifications can be registered in, and retrieved from a Redis database.
       */
      case "fields"   => context.actorOf(Props(new FieldQuestor(Configuration)))
      case "register" => context.actorOf(Props(new FieldRegistrar(Configuration)))        

      /*
       * Retrieve all the relations or rules discovered by a 
       * previous mining task; relevant is the 'uid' of the 
       * mining task to get the respective data
       */       
      case "get" => context.actorOf(Props(new RuleQuestor()))
      /*
       * Request to prepare the Elasticsearch index for subsequent
       * tracking events; this is an event invoked by the admin
       * interface
       */  
      case "index" => context.actorOf(Props(new BaseIndexer(Configuration)))
      /*
       * Request to track an item for later association rule mining; tracking
       * functionality is part of the core functionality.
       * 
       */   
      case "track" => context.actorOf(Props(new BaseTracker(Configuration)))
      /*
       * Request the actual status of an association rule mining 
       * task; note, that get requests should only be invoked after 
       * having retrieved a FINISHED status.
       * 
       * Status management is part of the core functionality.
       */
      case "status" => context.actorOf(Props(new StatusQuestor(Configuration)))
      /*
       * Start association rule mining
       */ 
      case "train" => context.actorOf(Props(new RuleMiner(sc)))
       
      case _ => throw new Exception("Task is unknown.")
      
    }
  
  }

}