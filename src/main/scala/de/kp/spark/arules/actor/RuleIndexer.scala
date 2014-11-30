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

import de.kp.spark.core.model._
import de.kp.spark.core.io.ElasticIndexer

import de.kp.spark.arules.model._
import de.kp.spark.arules.io.{ElasticBuilderFactory => EBF}

/**
 * RuleIndexer supports the administration task of creating an
 * Elasticsearch index, both for the registration of purchase
 * items and association rules
 */
class RuleIndexer extends BaseActor {
  
  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")
      val origin = sender

      try {

        val index   = req.data("index")
        val mapping = req.data("type")
    
        val topic = req.task.split(":")(1) match {
          
          case "item" => "item"
          
          case "rule" => "rule"
          
          case _ => {
            
            val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
            throw new Exception(msg)
            
          }
        
        }
        
        val builder = EBF.getBuilder(topic,mapping)
        val indexer = new ElasticIndexer()
    
        indexer.create(index,mapping,builder)
        indexer.close()
      
        val data = Map("uid" -> uid, "message" -> Messages.SEARCH_INDEX_CREATED(uid))
        val response = new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)	
      
        origin ! response
      
      } catch {
        
        case e:Exception => {
          
          log.error(e, e.getMessage())
      
          val data = Map("uid" -> uid, "message" -> e.getMessage())
          val response = new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
      
          origin ! response
          
        }
      
      } finally {
        
        context.stop(self)

      }
    }
    
  }

}