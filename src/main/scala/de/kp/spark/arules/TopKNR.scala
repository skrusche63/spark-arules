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

import de.kp.core.arules.{TopKNRAlgorithm,RuleG}

import de.kp.spark.arules.util.VerticalBuilder
import scala.collection.JavaConversions._

object TopKNR {
  
  def extractRules(sc:SparkContext,input:String,k:Int,minconf:Double,delta:Int, stats:Boolean=true):List[RuleG] = {
          
    val vertical = VerticalBuilder.build(sc,input)    
	
    /**
     * Run algorithm and create Top K association rules
     */
	val algo = new TopKNRAlgorithm()
	val rules = algo.runAlgorithm(k, minconf, vertical, delta)

	if (stats) algo.printStats()
    
    rules.toList
    
  }

  def rulesToJson(rules:List[RuleG]):String = {
    
    String.format("""{"rules":[%s]}""", rules.map(rule => {
			
      val antecedent = rule.getItemset1().toList
      val consequent = rule.getItemset2().toList

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new Rule(antecedent,consequent,support,confidence).toJSON
	
    }).mkString(","))

  }

}