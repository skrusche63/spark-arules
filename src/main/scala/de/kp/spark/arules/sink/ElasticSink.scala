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

import java.util.{Date,UUID}

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.io.ElasticWriter

import de.kp.spark.arules.model._

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class ElasticSink {

  def addRules(req:ServiceRequest,rules:Rules) {

    val uid = req.data(Names.REQ_UID)

    val index   = req.data(Names.REQ_SINK_INDEX)
    val mapping = req.data(Names.REQ_SINK_TYPE)
    
    val writer = new ElasticWriter()
    
    val readyToWrite = writer.open(index,mapping)
    if (readyToWrite == false) {
      
      writer.close()
      
      val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
      throw new Exception(msg)
      
    }
    
    /*
     * Determine timestamp for the actual set of rules to be indexed
     */
    val now = new Date()
    val timestamp = now.getTime()
   
    for (rule <- rules.items) {

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
      for (item <- rule.antecedent) {
      
        val source = new java.util.HashMap[String,Object]()    
      
        source += Names.TIMESTAMP_FIELD -> timestamp.asInstanceOf[Object]
        source += Names.UID_FIELD -> uid
      
        source += Names.RULE_FIELD -> rid
        
        source += Names.ANTECEDENT_FIELD -> item.asInstanceOf[Object]
        source += Names.CONSEQUENT_FIELD -> rule.consequent
        
        source += Names.SUPPORT_FIELD -> rule.support.asInstanceOf[Object]
        source += Names.CONFIDENCE_FIELD -> rule.confidence.asInstanceOf[Object]
        
        source += Names.WEIGHT_FIELD -> (1.toDouble / rule.antecedent.length).asInstanceOf[Object]
        
        /*
         * Writing this source to the respective index throws an
         * exception in case of an error; note, that the writer is
         * automatically closed 
         */
        writer.write(index, mapping, source)
        
      }
      
    }
    
    writer.close()
    
  }

}