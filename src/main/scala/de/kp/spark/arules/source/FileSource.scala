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

class FileSource(sc:SparkContext) extends Serializable {

  /**
   * Read data from file system: it is expected that the lines with
   * the respective text file are already formatted in the SPMF form
   */
  def connect(input:String):RDD[(Int,Array[Int])] = {
    
    sc.textFile(input).map(valu => {
      
      val Array(sid,sequence) = valu.split(",")  
      (sid.toInt,sequence.split(" ").map(_.toInt))
    
    }).cache
    
  }
  
}