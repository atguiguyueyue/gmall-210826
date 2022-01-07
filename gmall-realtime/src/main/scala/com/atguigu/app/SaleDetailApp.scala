package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.消费kafka中的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,ssc)

    //4.将数据转为样例类
    val orderInfoDStream = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id,orderInfo)
      })
    })

    val orderDetailDStream = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id,orderDetail)
      })
    })

//    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)

    //5.使用外连接连接两条流，为了方式连接不上时数据丢失
    val fulljoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.通过加缓存的方式解决数据因网络延迟所导致数据丢失问题
    val noUserSaleDetailDStream: DStream[SaleDetail] = fulljoinDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      //创建存放SaleDetail的集合
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建Redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      partition.foreach { case (orderId, (infoOpt, detailOpt)) =>

        //orderInfo对应redis的key
        val infoRedisKey: String = "OrderInfo:" + orderId
        //orderDetail对应redis的key
        val detailRedisKey: String = "OrderDetail:" + orderId

        //a.1判断orderInfo是否存在
        if (infoOpt.isDefined) {
          //orderInfo存在
          //提取orderInfo
          val orderInfo: OrderInfo = infoOpt.get

          //a.2判断OrderDetail数据是否存在
          if (detailOpt.isDefined) {
            //OrderDetail数据存在
            //取出OrderDetail数据
            val orderDetail: OrderDetail = detailOpt.get
            val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            //a.3将关联起来的数据存入结果集合中
            details.add(detail)
          }

          //b.1将orderInfo数据转为JSON写入redis中
          //          JSON.toJSONString(orderInfo)
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedis.set(infoRedisKey, orderInfoJson)
          //b.2为了防止数据积压，为期设置过期时间
          jedis.expire(infoRedisKey, 30)

          //c.1查询OrderDetail缓存中是否有对应的数据
          //判断redis中是否存有orderId对应的OrderDetail数据
          if (jedis.exists(detailRedisKey)) {
            //存在对应的数据
            //获取redis中存放的数据
            val orderDetailJsonSet: util.Set[String] = jedis.smembers(detailRedisKey)
            for (elem <- orderDetailJsonSet.asScala) {
              //将查询出来的OrderDetail对应的JSON字符串转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              //写入SaleDetail中
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            }
          }

        } else {
          //orderInfo不在
          //c.1判断OrderDetail是否存在
          if (detailOpt.isDefined) {
            //OrderDetail在
            //取出OrderDetail数据
            val orderDetail: OrderDetail = detailOpt.get

            //c.2去orderInfo缓存中查询是否有能关联上的数据
            if (jedis.exists(infoRedisKey)) {
              //有能关联上的OrderInfo数据
              val orderInfoJsonStr: String = jedis.get(infoRedisKey)
              //将字符串转为样例类
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
              //关联到结果集合
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            } else {
              //orderInfo缓存中没有能关联上的数据
              //c.3将自己（OrderDetail）存入缓存中
              //将OrderDetail样例类转为JSON字符串
              val orderDetailJSONStr: String = Serialization.write(orderDetail)
              jedis.sadd(detailRedisKey, orderDetailJSONStr)
              //设置过期时间
              jedis.expire(detailRedisKey, 30)
            }
          }
        }
      }
      jedis.close()
      details.asScala.toIterator
    })

    //7.反查缓存补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(partition => {
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //查询redis中用户表的数据
        val userInfoRedisKey: String = "UserInfo:" + saleDetail.user_id
        val userInfoJSONStr: String = jedis.get(userInfoRedisKey)

        //将查询出来的JSON数据转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJSONStr, classOf[UserInfo])

        //补全用户信息
        saleDetail.mergeUserInfo(userInfo)

        saleDetail
      })
      jedis.close()
      details
    })
    saleDetailDStream.print()

    //8.将数据保存至ES
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_SALE_DETAIL_INDEX+"0826",list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
