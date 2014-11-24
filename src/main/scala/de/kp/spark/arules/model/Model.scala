package de.kp.spark.arules.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.core.model._
import de.kp.spark.core.model.BaseMessages

/*
 * Service requests are mapped onto job descriptions and are stored
 * in a Redis instance
 */
case class JobDesc(
  service:String,task:String,status:String
)

case class Rule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double)

case class Rules(items:List[Rule])
/**
 * A derived association rule that additionally specifies the matching weight
 * between the antecent field and the respective field in mined and original
 * association rules
 */
case class WeightedRule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double,weight:Double)
/**
 * A set of weighted rules assigned to a certain user of a specific site
 */
case class UserRules(site:String,user:String,items:List[WeightedRule])

case class MultiUserRules(items:List[UserRules])

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def serializeFields(fields:Fields):String = write(fields)
  def deserializeFields(fields:String):Fields = read[Fields](fields)

  def serializeJob(job:JobDesc):String = write(job)
  def deserializeJob(job:String):JobDesc = read[JobDesc](job)
  
  def serializeResponse(response:ServiceResponse):String = write(response)
  
  def deserializeRequest(request:String):ServiceRequest = read[ServiceRequest](request)
  def serializeRequest(request:ServiceRequest):String = write(request)
  
  def serializeWeightedRules(rules:UserRules):String = write(rules)
  def deserializeWeightedRules(rules:String):UserRules = read[UserRules](rules)

  def serializeMultiUserRules(rules:MultiUserRules):String = write(rules)
  def deserializeMultiUserRules(rules:String):MultiUserRules = read[MultiUserRules](rules)
  
  def serializeRules(rules:Rules):String = write(rules)  
  def deserializeRules(rules:String):Rules = read[Rules](rules)
  
}

object Algorithms {
  /*
   * Association Analysis supports two different mining algorithms:
   * TOPK and TOPKNR; both algorithms have a strong focus on the top
   * rules and avoid the so called "threshold" problem. This makes it
   * a lot easier to directly use the mining results.
   */
  val TOPK:String   = "TOPK"
  val TOPKNR:String = "TOPKNR"
    
  private val algorithms = List(TOPK,TOPKNR)
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
}

object Sources {

  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PIWIK:String   = "PIWIK"    
  
  private val sources = List(FILE,ELASTIC,JDBC,PIWIK)
  def isSource(source:String):Boolean = sources.contains(source)
  
}

object Sinks {
  
  val ELASTIC:String = "ELASTIC"
  val JDBC:String    = "JDBC"
    
  private val sinks = List(ELASTIC,JDBC)
  def isSink(sink:String):Boolean = sinks.contains(sink)
  
}

object Messages extends BaseMessages {

  def MINING_STARTED(uid:String) = 
    String.format("""[UID: %s] Training task started.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = 
    String.format("""[UID: %s] Training task has missing parameters.""", uid)

  def NO_ITEMS_PROVIDED(uid:String):String = 
    String.format("""[UID: %s] No items are provided.""", uid)

  def RULES_DO_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] No association rules found.""", uid)

  def SEARCH_INDEX_CREATED(uid:String):String = 
    String.format("""[UID: %s] Search index created.""", uid)
 
  def TRACKED_ITEM_RECEIVED(uid:String):String = 
    String.format("""[UID: %s] Tracked item(s) received.""", uid)
 
}

object ResponseStatus {
  
  val DATASET:String = "dataset"
  /*
   * Stati to describe start & end of a longer
   * running mining task
   */
  val MINING_STARTED:String  = "mining:started"
  val MINING_FINISHED:String = "mining:finished"
  
  val RULES:String = "rules"
    
  val TRACKING_STARTED:String = "tracking:started"
  val TRACKING_FINISHED:String = "tracking:finsihed"
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}