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

import akka.actor.{ActorSystem,Props}
import com.typesafe.config.ConfigFactory

import de.kp.spark.arules.actor.ARulesMaster

/**
 * ARulesService is an Akka Remoting based Association Rule
 * Mining Service that actually supports Top-K and Top-K
 * non redundant algorithms
 */
object ARulesService {

  def main(args: Array[String]) {
    
    val name:String = "arules-server"
    val conf:String = "server.conf"

    val server = new ARulesService(conf, name)
    while (true) {}
    
    server.shutdown
      
  }

}

class ARulesService(conf:String, name:String) {

  val system = ActorSystem(name, ConfigFactory.load(conf))
  sys.addShutdownHook(system.shutdown)

  val master = system.actorOf(Props[ARulesMaster], name="arules-master")

  def shutdown = system.shutdown()
  
}