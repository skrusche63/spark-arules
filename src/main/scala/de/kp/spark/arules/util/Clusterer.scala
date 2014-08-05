package de.kp.spark.arules.util
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

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}

object Clusterer {

  def buildKMeansClusters(sc:SparkContext, input:String,clusters:Int,iterations:Int):RDD[String] = {
    
    val file = sc.textFile(input).cache
    
    /**
     * Convert dataset into vectors and train KMeans model
     */
    val ids = file.flatMap(line => line.split(" ").map(_.toInt)).collect()
     
    val maxid = ids.max
    val minid = ids.min
     
    val len = sc.broadcast(maxid - minid + 1)
    val min = sc.broadcast(minid)
     
    val vectors = file.map(line => {

      val vec = Array.fill[Double](len.value)(0)
      val ids = line.split(" ").map(_.toInt)
       
      ids.foreach(id => {
         
        val pos = id - min.value
        vec(pos) = vec(pos) + 1
         
      })
       
      Vectors.dense(vec)
       
    })
    
    val model = KMeans.train(vectors, clusters, iterations)
    /**
     *  Apply model
     */ 
    val clusteredSource = file.map(line => {
      
      val vec = Array.fill[Double](len.value)(0)
      val ids = line.split(" ").map(_.toInt)
       
      ids.foreach(id => {
         
        val pos = id - min.value
        vec(pos) = vec(pos) + 1
         
      })
       
      val cluster = model.predict(Vectors.dense(vec))
      cluster + "|" + line
      
    })
    
    clusteredSource
    
  }
}