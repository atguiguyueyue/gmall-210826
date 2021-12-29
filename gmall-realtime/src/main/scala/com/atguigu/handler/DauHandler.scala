package com.atguigu.handler

import java.{lang, util}

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 批次间去重
    * @param startUpLogDStream
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
   /* val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      //1.获取redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      //2.获取redis中的数据
      val redisKey: String = "DAU:" + log.logDate
      //      val mids: util.Set[String] = jedis.smembers(redisKey)
      //3.判断当前的mid是否包含在redis中
      //      val bool: Boolean = mids.contains(log.mid)
      //3.直接利用redis中Set类型的方法来判断是否存在
      val bool: lang.Boolean = jedis.sismember(redisKey, log.mid)
      jedis.close()
      !bool
    })
     value
    */
    //方案二：在每个分区下获取连接
    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //创建redis连接
      println("创建redis连接")
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = partition.filter(log => {
        //2.获取redis中的数据
        val redisKey: String = "DAU:" + log.logDate
        //      val mids: util.Set[String] = jedis.smembers(redisKey)
        //3.判断当前的mid是否包含在redis中
        //      val bool: Boolean = mids.contains(log.mid)
        //3.直接利用redis中Set类型的方法来判断是否存在
        val bool: lang.Boolean = jedis.sismember(redisKey, log.mid)
        !bool
      })
      jedis.close()
      logs
    })
    value


  }

  /**
    * 将mid保存至Redis
    *
    * @param startUpLogDStream
    */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //创建redis连接
        val jedis: Jedis = new Jedis("hadoop102",6379)
        partition.foreach(startUpLog=>{
          //将数据写入Redis中
          val redisKey: String = "DAU:"+startUpLog.logDate
          jedis.sadd(redisKey,startUpLog.mid)
        })
        //关闭连接
        jedis.close()
      })
    })

  }

}
