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

import akka.actor.{ActorRef,Props}

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.arules.RequestContext

class RuleMaster(@transient ctx:RequestContext) extends BaseMaster(ctx.config) {
  
  protected def actor(worker:String):ActorRef = {
    
    worker match {
      /*
       * Metadata management is part of the core functionality; field or metadata
       * specifications can be registered in, and retrieved from a Redis database.
       */
      case "fields"   => context.actorOf(Props(new FieldQuestor(ctx.config)))
      case "register" => context.actorOf(Props(new BaseRegistrar(ctx.config)))        
      /*
       * Index management is part of the core functionality; an Elasticsearch 
       * index can be created and appropriate (tracked) items can be saved.
       */  
      case "index" => context.actorOf(Props(new BaseIndexer(ctx.config)))
      case "track" => context.actorOf(Props(new BaseTracker(ctx.config)))

      case "params" => context.actorOf(Props(new ParamQuestor(ctx.config)))
      /*
       * Request the actual status of an association rule mining 
       * task; note, that get requests should only be invoked after 
       * having retrieved a FINISHED status.
       * 
       * Status management is part of the core functionality.
       */
      case "status" => context.actorOf(Props(new StatusQuestor(ctx.config)))
      /*
       * Retrieve all the relations or rules discovered by a 
       * previous mining task; relevant is the 'uid' of the 
       * mining task to get the respective data
       */       
      case "get" => context.actorOf(Props(new RuleQuestor()))
      /*
       * Start association rule mining
       */ 
      case "train" => context.actorOf(Props(new RuleMiner(ctx)))
       
      case _ => throw new Exception("Task is unknown.")
      
    }
  
  }

}