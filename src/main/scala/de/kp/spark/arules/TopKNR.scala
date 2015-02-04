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

import org.apache.spark.rdd.RDD
import de.kp.core.arules._

import de.kp.spark.core.model._
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
  
  def extractRules(dataset:RDD[(Int,Array[Int])],k:Int,minconf:Double,delta:Int,stats:Boolean=true):List[RuleG] = {
    
    new TopKNR().extractRDDRules(dataset,k,minconf,delta,stats)

  }

}