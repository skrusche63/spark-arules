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

import de.kp.spark.arules.model._
import de.kp.spark.arules.redis.RedisCache

import scala.collection.mutable.ArrayBuffer

class RuleRegistrar extends Actor with ActorLogging {
  
  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")
      
      val response = try {
        
        /* Unpack fields from request and register in Redis instance */
        val fields = ArrayBuffer.empty[Field]

        fields += new Field("site","string",req.data("site"))
        fields += new Field("timestamp","long",req.data("timestamp"))

        fields += new Field("user","string",req.data("user"))
        fields += new Field("group","string",req.data("group"))

        fields += new Field("item","integer",req.data("integer"))
        RedisCache.addFields(req, new Fields(fields.toList))
        
        new ServiceResponse("association","meta",Map("uid"-> uid),ARulesStatus.SUCCESS)
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! Serializer.serializeResponse(response)

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