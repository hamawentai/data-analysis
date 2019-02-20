package com.lab.analysis

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisPool {

  private val jedisPoolConfig = new JedisPoolConfig
  jedisPoolConfig.setMaxTotal(10)
  private val redisPool: JedisPool = new JedisPool(jedisPoolConfig, "hadoop3", 6379, 100000, "123456")

  def getRedisPool: JedisPool = redisPool

  def main(args: Array[String]): Unit = {
   val redis = redisPool.getResource
    println(redis.keys("*"))
    redis.close()
  }
}
