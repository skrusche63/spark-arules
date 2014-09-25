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

import de.kp.spark.arules.model.Sources

/**
 * A TransactionSource is an abstraction layer on top of
 * different physical data source to retrieve a transaction
 * database compatible with the Top-K and Top-KNR algorithm
 */
class TransactionSource(@transient sc:SparkContext) {

  def get(data:Map[String,String]):RDD[(Int,Array[Int])] = {
    
    val source = data("source")
    source match {
      /* 
       * Discover top k association rules from transaction database persisted 
       * as an appropriate search index from Elasticsearch; the configuration
       * parameters are retrieved from the service configuration 
       */    
      case Sources.ELASTIC => new ElasticSource(sc).connect(data)
      /* 
       * Discover top k association rules from transaction database persisted 
       * as a file on the (HDFS) file system; the configuration parameters are 
       * retrieved from the service configuration  
       */    
      case Sources.FILE => new FileSource(sc).connect(data)
      /*
       * Retrieve Top-K association rules from transaction database persisted 
       * as an appropriate table from a JDBC database; the configuration parameters 
       * are retrieved from the service configuration
       */
      case Sources.JDBC => new JdbcSource(sc).connect(data)
      /*
       * Retrieve Top-K association rules from transaction database persisted 
       * as an appropriate table from a Piwik database; the configuration parameters 
       * are retrieved from the service configuration
       */
      case Sources.PIWIK => new PiwikSource(sc).connect(data)
            
   }

  }
  
}