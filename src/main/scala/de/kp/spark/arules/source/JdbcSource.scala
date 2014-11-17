package de.kp.spark.arules.source
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

import de.kp.spark.arules.Configuration
import de.kp.spark.arules.io.JdbcReader

import de.kp.spark.arules.spec.Fields

/**
 * JdbcSource is a common connector to Jdbc databases; it uses a common
 * field specification to retrieve the respective transaction database
 * and is independent of the interpretation of an 'item': An item may be
 * an article of a publisher site, a product or service of a retailer site,
 * and also a customer state to specify a certain behavioral state.
 */
class JdbcSource(@transient sc:SparkContext) {
  
  def connect(params:Map[String,Any]):RDD[Map[String,Any]] = {
    
    val uid = params("uid").asInstanceOf[String]    
    
    val fieldspec = Fields.get(uid)
    val fields = fieldspec.map(kv => kv._2._1).toList    
    
    /* Retrieve site and query from params */
    val site = params("site").asInstanceOf[Int]
    val query = params("query").asInstanceOf[String]

    new JdbcReader(sc,site,query).read(fields)

  }

}