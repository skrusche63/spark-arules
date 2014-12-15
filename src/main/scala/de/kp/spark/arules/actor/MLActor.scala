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
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.arules.{Configuration,RemoteContext}

import de.kp.spark.arules.model._
import de.kp.spark.arules.sink._

/**
 * MLActor comprises common functionality for the algorithm specific
 * actors, TopKActor and TopKNRActor
 */
abstract class MLActor(@transient sc:SparkContext) extends BaseActor {
  
  /**
   * For every (site,user) pair and every discovered association rule, 
   * determine the 'antecedent' intersection ratio and filter those
   * above a user defined threshold, and restrict to those rules, where
   * the transaction 'items' do not intersect with the 'consequents' 
   */  
  protected def findMultiUserRules(req:ServiceRequest,dataset:RDD[(String,String,List[Int])],rules:List[Rule],weight:Double) {
    
    val sc = dataset.context
    
    val bcrules = sc.broadcast(rules)
    val bcweight = sc.broadcast(weight)
              
    val multiUserRules = dataset.map(itemset => {
                
      val (site,user,items) = itemset
      val userRules = bcrules.value.map(rule => {

        val intersect = items.intersect(rule.antecedent)
        val ratio = intersect.length.toDouble / items.length
                  
        new WeightedRule(items,rule.consequent,rule.support,rule.confidence,ratio)
        /*
         * Restrict to rules, where a) the intersection ratio is above the
         * externally provided threshold ('weight') and b) where no items also
         * appear as consequents of the respective rules
         */
      }).filter(r => (r.weight > bcweight.value) && (r.antecedent.intersect(r.consequent).size == 0))
                
      new UserRules(site,user,userRules)
                
    }).collect()
          
    saveMultiUserRules(req,new MultiUserRules(multiUserRules.toList))
          
    /* Update RedisCache */
    cache.addStatus(req,ResponseStatus.MINING_FINISHED)

    /* Notify potential listeners */
    notify(req,ResponseStatus.MINING_FINISHED)
    
  }
  
  /**
   * (Multi-)User Rules are actually registered in an internal Redis instance
   * only; note, that is different from rules, which are also indexed in an
   * Elasticsearch index
   */
  protected def saveMultiUserRules(req:ServiceRequest, rules:MultiUserRules) {

    val sink = new RedisSink()
    sink.addUserRules(req,rules)
    
  }

  /**
   * Rules are actually registered in an internal Redis instance as well
   * as in an Elasticsearch index or a Jdbc database
   */
  protected def saveRules(req:ServiceRequest,rules:Rules) {
    
    /*
     * Discovered rules are always registered in an internal Redis instance;
     * on a per request basis, additional data sinks may be used; this depends
     * on whether a cetain sink has been specified with the request
     */
    val redis = new RedisSink()
    redis.addRules(req,rules)
    
    if (req.data.contains(Names.REQ_SINK) == false) return
    
    val sink = req.data(Names.REQ_SINK)
    if (Sinks.isSink(sink) == false) return
    
    sink match {
      
      case Sinks.ELASTIC => {
    
        val elastic = new ElasticSink()
        elastic.addRules(req,rules)
        
      }
      
      case Sinks.JDBC => {
            
        val jdbc = new JdbcSink()
        jdbc.addRules(req,rules)
 
      }

      case Sinks.PARQUET => {
            
        val parquet = new ParquetSink(sc)
        parquet.addRules(req,rules)
 
      }
      
      case _ => {/* do nothing */}
      
    }
    
  }

  /**
   * Notify all registered listeners about a certain status
   */
  protected def notify(req:ServiceRequest,status:String) {

    /* Build message */
    val response = new ServiceResponse(req.service,req.task,req.data,status)	
    
    /* Notify listeners */
    val message = serialize(response)    
    RemoteContext.notify(message)
    
  }
  
  protected def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data(Names.REQ_UID)
    
    if (missing == true) {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
  
    } else {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MINING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.MINING_STARTED)	
  
    }

  }

}