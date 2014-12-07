package de.kp.spark.arules.api
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

import java.util.Date

import org.apache.spark.SparkContext

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._

import spray.routing.{Directives,HttpService,RequestContext,Route}

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.core.model._
import de.kp.spark.core.rest.RestService

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.actor.{RuleMaster}
import de.kp.spark.arules.model._

class RestApi(host:String,port:Int,system:ActorSystem,@transient val sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.core.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system
  
  val (duration,retries,time) = Configuration.actor   
  val master = system.actorOf(Props(new RuleMaster(sc)), name="association-master")

  private val service = "association"
 
  def start() {
    RestService.start(routes,system,host,port)
  }

  private def routes:Route = {
    /*
     * A 'fields' request supports the retrieval of the field
     * or metadata specificiations that are associated with
     * a certain training task (uid).
     * 
     * The approach actually supported enables the registration
     * of field specifications on a per uid basis, i.e. each
     * task may have its own fields. Requests that have to
     * refer to the same fields must provide the SAME uid
     */
    path("fields") {  
	  post {
	    respondWithStatus(OK) {
	      ctx => doFields(ctx)
	    }
	  }
    }  ~  
    /*
     * A 'register' request supports the registration of a field
     * or metadata specification that describes the fields used
     * to span the training dataset.
     */
    path("register") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRegister(ctx)
	    }
	  }
    }  ~ 
    /*
     * 'index' and 'track' requests refer to the tracking functionality
     * of the Association Analysis engine; while 'index' prepares a
     * certain Elasticsearch index, 'track' is used to gather training
     * data.
     */
    path("index" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doIndex(ctx,subject)
	    }
	  }
    }  ~ 
    path("track") {
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrack(ctx)
	    }
	  }
    }  ~  
    /*
     * A 'status' request supports the retrieval of the status
     * with respect to a certain training task (uid). The latest
     * status or all stati of a certain task are returned.
     */
    path("status" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,subject)
	    }
	  }
    }  ~ 
    path("get" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,subject)
	    }
	  }
    }  ~ 
    path("train") {
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx)
	    }
	  }
    }  ~ 
    pathPrefix("web") {
      /*
       * 'web' is the prefix for static public content that is
       * served from a web browser and provides a minimalistic
       * web UI for this prediction server
       */
      implicit val actorContext = actorRefFactory
      get {
	    respondWithStatus(OK) {
	      getFromResourceDirectory("public")
	    }
      }
    }
  }

  private def doGet[T](ctx:RequestContext,subject:String) = {
	    
	subject match {

	  /*
	   * 'antecedent' retrieves those association rules where an externally
	   * provided itemset matches the antecedent part of the association rules
	   */
	  case "antecedent" => doRequest(ctx,service,"get:antecedent")
	  /*
	   * 'consequent' retrieves those association rules where an externally
	   * provided itemset matches the consequent part of the association rules
	   */
	  case "consequent" => doRequest(ctx,service,"get:consequent")
      /*
       * 'transaction' retrieves those association rules where the discovered
       * antecedent part (within the rules) matches items of the last
       * transaction
       */
	  case "transaction" => doRequest(ctx,service,"get:transaction")
	  /*
	   * 'rule' retrieves the discovered association rules without any data 
	   * aggregation or transformation
	   */
	  case "rule" => doRequest(ctx,service,"get:rule")
	      
	  case _ => {}

	}
    
  }

  private def doTrain[T](ctx:RequestContext) = doRequest(ctx,service,"train")

  /**
   * 'fields' and 'register' requests refer to the metadata management 
   * of the Association Analysis engine; for a certain task (uid) and 
   * a specific model (name), a specification of the respective data fields 
   * can be registered and retrieved from a Redis database.
   */
  private def doFields[T](ctx:RequestContext) = doRequest(ctx,service,"fields:item")
  
  private def doRegister[T](ctx:RequestContext) = doRequest(ctx,service,"register:item")
  
  /**
   * 'index' & 'track' requests support data registration in an Elasticsearch
   * index; while items are can be provided via the REST interface, rules are
   * built by the Association Analysis engine and then registered in the index.
   */
  private def doIndex[T](ctx:RequestContext,subject:String) = {
    
    subject match {
      
      case "item" => doRequest(ctx,service,"index:item")
      case "rule" => doRequest(ctx,service,"index:rule")
      
      case _ => {}
      
    }
    
  }
  
  private def doTrack[T](ctx:RequestContext) = doRequest(ctx,service,"track:item")

  /**
   * 'status' is an administration request to determine whether a certain data
   * mining task has been finished or not; the only parameter required for status 
   * requests is the unique identifier of a certain task
   */
  private def doStatus[T](ctx:RequestContext,subject:String) = {
    
    subject match {
      /*
       * Retrieve the 'latest' status information about a certain
       * data mining or model building task.
       */
      case "latest" => doRequest(ctx,service,"status:latest")
      /*
       * Retrieve 'all' stati assigned to a certain data mining
       * or model building task.
       */
      case "all" => doRequest(ctx,service,"status:all")
      
      case _ => {/* do nothing */}
    
    }
  
  }
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String) = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master,request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }

}