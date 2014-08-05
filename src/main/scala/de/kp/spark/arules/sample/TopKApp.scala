package de.kp.spark.arules.sample
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

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.serializer.KryoSerializer

import de.kp.spark.arules.TopK
import de.kp.spark.arules.util.SPMFBuilder

object TopKApp {
  
  private val prepare = false
  
  def main(args:Array[String]) {

    val input  = "/Work/tmp/arules/input/ContextIGB.txt"
    val output = "/Work/tmp/arules/output/ContextIGB"
    
    var start = System.currentTimeMillis()
    
    val sc = createLocalCtx("TopKApp")
    
    if (prepare) {
      
      SPMFBuilder.build(sc, input, Some(output))
      println("Prepare Time: " + (System.currentTimeMillis() - start) + " ms")
      
      start = System.currentTimeMillis()
      
    }
    
    val k = 10
    val minconf = 0.8
    
    val rules = TopK.extractRules(sc,output,k,minconf)
    println(TopK.rulesToJson(rules))
    
    println("===================================================")
    
	for (rule <- rules) {
			
	  val buf = new StringBuffer();
	  buf.append(rule.toString())
			
	  buf.append(" #SUP: ");
      buf.append(rule.getAbsoluteSupport());
		
      buf.append(" #CONF: ");
      buf.append(rule.getConfidence());

	  println(buf.toString)	
	
	}
    
    println("===================================================")

    val end = System.currentTimeMillis()
    println("Total time: " + (end-start) + " ms")

  }
  
  private def createLocalCtx(name:String):SparkContext = {

	System.setProperty("spark.executor.memory", "4g")
	System.setProperty("spark.kryoserializer.buffer.mb","256")
	/**
	 * Other configurations
	 * 
	 * System.setProperty("spark.cores.max", "532")
	 * System.setProperty("spark.default.parallelism", "256")
	 * System.setProperty("spark.akka.frameSize", "1024")
	 * 
	 */	
    val runtime = Runtime.getRuntime()
	runtime.gc()
		
	val cores = runtime.availableProcessors()
		
	val conf = new SparkConf()
	conf.setMaster("local["+cores+"]")
		
	conf.setAppName(name);
    conf.set("spark.serializer", classOf[KryoSerializer].getName)		
        
	new SparkContext(conf)
		
  }

}