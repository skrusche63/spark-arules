package de.kp.spark.arules.redis

import redis.clients.jedis.Jedis
import de.kp.spark.arules.Configuration

object RedisClient {

  def apply():Jedis = {

    val (host,port) = Configuration.redis
    new Jedis(host,port.toInt)
    
  }
  
}