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

import de.kp.spark.arules.Rule

case class ARulesAntecedent(
  items:List[Integer]
)

case class ARulesParameters(
  /*
   * Number of rules to be returned by the algorithm
   */
  k:Int,
  /*
   * Minimum confidence to be used by the algorithm
   */
  minconf:Double,
  delta:Option[Int]    
)

case class ARulesRequest(
  /* 
   * Unique identifier to distinguish requests from each other;
   * the request is responsible for setting appropriate identifiers
   */
  uid:String,
  /*
   * The task of the request: for mining requests, two different task 
   * are supported:
   * 
   * a) start:   start a specific mining job
   * b) status:  get actual status of mining job
   */
  task:String,
  /*
   * The algorithm to be used when starting a specific mining job;
   * actually two different ARM algorithms are supported: TOPK & TOPKNR
   */
  algorithm:Option[String],
  /*
   * The parameters either for the Top-K or Top-KNR algorithm
   */
  parameters:Option[ARulesParameters],
  /*
   * The antecedent candidates to be used in a recommendation request
   */
  antecedent:Option[ARulesAntecedent],
  source:Option[ARulesSource]
)

case class ARulesResponse(
  /*
   * Unique identifier of the request, this response belongs to
   */
  uid:String,
  /*
   * Message
   */
  message:Option[String],
  /*
   * The rules computed for the respective job
   */
  rules:Option[List[Rule]],
  /*
   * The consequent computed for the respective job
   */
  consequent:Option[List[Integer]],
  status:String
)

case class ElasticRequest()

case class FileRequest(
  path:String
)

/**
 * ARulesSource aggregates the access parameters of
 * file or elasticsearch based association rule sources
 */
case class ARulesSource(
  /*
   * The path to a file on the HDFS or local file system
   * that holds a textual description of a sequence database
   */
  path:Option[String]
)

object ARulesModel {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializeResponse(response:ARulesResponse):String = write(response)
  
  def deserializeRequest(request:String):ARulesRequest = read[ARulesRequest](request)
  
}

object ARulesAlgorithms {
  
  val TOPK:String   = "TOPK"
  val TOPKNR:String = "TOPKNR"
  
}

object ARulesMessages {

  def ALGORITHM_IS_UNKNOWN(uid:String,algorithm:String):String = String.format("""Algorithm '%s' is unknown for uid '%s'.""", algorithm, uid)
 
  def NO_ALGORITHM_PROVIDED(uid:String):String = String.format("""No algorithm provided for uid '%s'.""", uid)

  def NO_PARAMETERS_PROVIDED(uid:String):String = String.format("""No parameters provided for uid '%s'.""", uid)

  def NO_SOURCE_PROVIDED(uid:String):String = String.format("""No source provided for uid '%s'.""", uid)

  def ANTECEDENTS_DO_NOT_EXIST(uid:String):String = String.format("""No antecedents found for uid '%s'.""", uid)

  def TASK_ALREADY_STARTED(uid:String):String = String.format("""The task with uid '%s' is already started.""", uid)

  def TASK_DOES_NOT_EXIST(uid:String):String = String.format("""The task with uid '%s' does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = String.format("""The task '%s' is unknown for uid '%s'.""", task, uid)
  
  def TOP_K_MISSING_PARAMETERS(uid:String):String = String.format("""Top-K parameter k or minconf is missing for uid '%s'.""", uid)
  
  def TOP_KNR_MISSING_PARAMETERS(uid:String):String = String.format("""Top-K parameter k, minconf or delta is missing for uid '%s'.""", uid)

  def TOP_K_MINING_STARTED(uid:String) = String.format("""Top-K Association Rule Mining started for uid '%s'.""", uid)
  
  def TOP_KNR_MINING_STARTED(uid:String) = String.format("""Top-K non-redundant Association Rule Mining started for uid '%s'.""", uid)

  def RULES_DO_NOT_EXIST(uid:String):String = String.format("""The rules for uid '%s' do not exist.""", uid)
  
}

object ARulesStatus {
  
  val DATASET:String = "dataset"
    
  val STARTED:String = "started"
  val STOPPED:String = "stopped"
    
  val FINISHED:String = "finished"
  val RUNNING:String  = "running"
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}