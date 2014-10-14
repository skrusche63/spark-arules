package de.kp.spark.arules.rest
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

import akka.actor.ActorSystem

import de.kp.spark.arules.{Configuration,SparkService}

object RestServer extends SparkService {
  
  /* Create Spark context */
  private val sc = createCtxLocal("RuleContext",Configuration.spark)      
  
  private def start(args:Array[String],system:ActorSystem) {

    val (host,port) = Configuration.rest
    
    /* Start REST API */
    new RestApi(host,port,system,sc).start()
      
  }
  
  def main(args: Array[String]) {
    start(args, ActorSystem("RestServer"))
  }
  
}