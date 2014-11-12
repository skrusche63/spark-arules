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

case class Listener(
  timeout:Int, url:String
)
/**
 * ServiceRequest & ServiceResponse specify the content 
 * sent to and received from the decision service
 */
case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)
case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)
/*
 * The Field and Fields classes are used to specify the fields with
 * respect to the data source provided, that have to be mapped onto
 * site,timestamp,user,group,item
 */
case class Field(
  name:String,datatype:String,value:String
)
case class Fields(items:List[Field])
/*
 * Service requests are mapped onto job descriptions and are stored
 * in a Redis instance
 */
case class JobDesc(
  service:String,task:String,status:String
)

case class Relation (
  items:List[Int],related:List[Int],support:Int,confidence:Double,weight:Double)

case class Rule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double)

case class Relations(site:String,user:String,items:List[Relation])

case class MultiRelations(items:List[Relations])

case class Rules(items:List[Rule])

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def serializeFields(fields:Fields):String = write(fields)
  def deserializeFields(fields:String):Fields = read[Fields](fields)

  def serializeJob(job:JobDesc):String = write(job)
  def deserializeJob(job:String):JobDesc = read[JobDesc](job)
  
  def serializeResponse(response:ServiceResponse):String = write(response)
  
  def deserializeRequest(request:String):ServiceRequest = read[ServiceRequest](request)
  def serializeRequest(request:ServiceRequest):String = write(request)

  /*
   * Support for serialization and deserialization of relations
   */
  def serializeMultiRelations(relations:MultiRelations):String = write(relations)
  
  def serializeRelations(relations:Relations):String = write(relations)

  def deserializeMultiRelations(relations:String):MultiRelations = read[MultiRelations](relations)

  def deserializeRelations(relations:String):Relations = read[Relations](relations)
  
  /*
   * Support for serialization and deserialization of rules
   */
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

object Messages {

  def ALGORITHM_IS_UNKNOWN(uid:String,algorithm:String):String = String.format("""Algorithm '%s' is unknown for uid '%s'.""", algorithm, uid)

  def GENERAL_ERROR(uid:String):String = String.format("""A general error appeared for uid '%s'.""", uid)
 
  def NO_ALGORITHM_PROVIDED(uid:String):String = String.format("""No algorithm provided for uid '%s'.""", uid)

  def NO_PARAMETERS_PROVIDED(uid:String):String = String.format("""No parameters provided for uid '%s'.""", uid)

  def NO_SOURCE_PROVIDED(uid:String):String = String.format("""No source provided for uid '%s'.""", uid)

  def NO_ITEMS_PROVIDED(uid:String):String = String.format("""No items are provided for uid '%s'.""", uid)

  def REQUEST_IS_UNKNOWN():String = String.format("""Unknown request.""")

  def TASK_ALREADY_STARTED(uid:String):String = String.format("""The task with uid '%s' is already started.""", uid)

  def TASK_DOES_NOT_EXIST(uid:String):String = String.format("""The task with uid '%s' does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = String.format("""The task '%s' is unknown for uid '%s'.""", task, uid)

  def MINING_STARTED(uid:String) = String.format("""Association rule mining started for uid '%s'.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = String.format("""Missing parameters for uid '%s'.""", uid)

  def RELATIONS_DO_NOT_EXIST(uid:String):String = String.format("""The relations for uid '%s' do not exist.""", uid)

  def RULES_DO_NOT_EXIST(uid:String):String = String.format("""The rules for uid '%s' do not exist.""", uid)

  def SOURCE_IS_UNKNOWN(uid:String,source:String):String = String.format("""Source '%s' is unknown for uid '%s'.""", source, uid)
 
  def TRACKED_ITEM_RECEIVED(uid:String):String = String.format("""Tracked item received for uid '%s'.""", uid)
 
}

object ARulesStatus {
  
  val DATASET:String = "dataset"
    
  val STARTED:String = "started"
  val FINISHED:String = "finished"
  
  val RULES:String = "rules"
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}