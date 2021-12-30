package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util.Date
import java.{lang, util}

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 批次内去重
    * @param filterByRedisDStream
    */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    //1.将数据转为Kv类型
    val midWithLogDateToStartUpLogDStream: DStream[(String, StartUpLog)] = filterByRedisDStream.map(startUplog => {
      (startUplog.mid + startUplog.logDate, startUplog)
    })

    //2.将当天相同mid的数据聚和到一块
    val midWithLogDateToIterStartUpLogDStream: DStream[(String, Iterable[StartUpLog])] = midWithLogDateToStartUpLogDStream.groupByKey()

    //3.对迭代器中的数据做排序
    val midWithLogDateToListStartUpLogDStream: DStream[(String, List[StartUpLog])] = midWithLogDateToIterStartUpLogDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    //4.取出value，并打散
    val value: DStream[StartUpLog] = midWithLogDateToListStartUpLogDStream.flatMap(_._2)
    value
  }

  /**
    * 批次间去重
    *
    * @param startUpLogDStream
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
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
    /*val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
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
    value*/
    //方案三：在每个批次下获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value1: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      println("创建Redis连接")
      //创建Redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //在Driver端获取redis中的数据
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //将redis中查询出来的数据广播到Executor端
      val midsBc: Broadcast[util.Set[String]] = sc.broadcast(mids)

      val value: RDD[StartUpLog] = rdd.filter(log => {
        val bool: Boolean = midsBc.value.contains(log.mid)
        !bool
      })
      jedis.close()
      value
    })
    value1


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
