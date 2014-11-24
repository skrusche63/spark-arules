package de.kp.spark.arules
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
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import de.kp.core.arules.{TopKNRAlgorithm,RuleG,Vertical}
import de.kp.spark.core.source.FileSource

import de.kp.spark.arules.model._
import de.kp.spark.arules.source.{TransactionModel}

import scala.collection.JavaConversions._

class TopKNR {
  
  /**
   * Build vertical representation from external data format
   * and find Top K NR rules from vertical database
   */
  def extractRDDRules(dataset:RDD[(Int,Array[Int])],k:Int,minconf:Double,delta:Int,stats:Boolean=true):List[RuleG] = {
          
    val vertical = VerticalBuilder.build(dataset)    
    findRDDRules(vertical,k,minconf,delta,stats)
    
  }

  /**
   * Run algorithm from vertical database and create Top K association rules
   */
  def findRDDRules(vertical:Vertical,k:Int,minconf:Double,delta:Int,stats:Boolean=true):List[RuleG] = {

	val algo = new TopKNRAlgorithm()
	val rules = algo.runAlgorithm(k, minconf, vertical, delta)

	if (stats) algo.printStats()
    
    rules.toList
    
  }
  
}

object TopKNR {
  
  def extractFileRules(@transient sc:SparkContext,k:Int,minconf:Double,delta:Int,stats:Boolean=true):List[RuleG] = {
    
    val model = new TransactionModel(sc)

    val path = Configuration.file()
    val source = new FileSource(sc)

    val rawset = source.connect(null,path)
    val dataset = model.buildFile(null,rawset)
    
    new TopKNR().extractRDDRules(dataset,k,minconf,delta,stats)
    
  }
  
  def extractRules(dataset:RDD[(Int,Array[Int])],k:Int,minconf:Double,delta:Int,stats:Boolean=true):List[RuleG] = {
    
    new TopKNR().extractRDDRules(dataset,k,minconf,delta,stats)

  }

  def rulesToJson(rules:List[RuleG]):String = {
    
    val items = rules.map(rule => {
			
      val antecedent = rule.getItemset1().toList.map(_.toInt)
      val consequent = rule.getItemset2().toList.map(_.toInt)

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new Rule(antecedent,consequent,support,confidence)
	
    })
    
    Serializer.serializeRules(new Rules(items))

  }

}