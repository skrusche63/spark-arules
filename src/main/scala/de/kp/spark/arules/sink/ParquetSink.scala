package de.kp.spark.arules.sink
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

import java.util.{Date,UUID}

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.io.ParquetWriter

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.model._

class ParquetSink(@transient sc:SparkContext) {

  def addRules(req:ServiceRequest,rules:Rules) {
 
    val uid = req.data(Names.REQ_UID)
    val writer = new ParquetWriter(sc)
    /*
     * Determine timestamp for the actual set of rules to be saved
     */
    val now = new Date()
    val timestamp = now.getTime()
   
    val dataset = sc.parallelize(rules.items.flatMap(rule => {
      /* 
       * Unique identifier to group all entries 
       * that refer to the same rule
       */      
      val rid = UUID.randomUUID().toString()
      /*
       * The rule antecedents are indexed as single documents
       * with an additional weight, derived from the total number
       * of antecedents per rule
       */
      rule.antecedent.map(item => {
        val weight = (1.toDouble / rule.antecedent.length)
        RuleObject(timestamp,uid,rid,item,rule.consequent,rule.support,rule.confidence,weight)
      })
      
    }))

    writer.writeRules(Configuration.input(0), dataset)
    
  }

}