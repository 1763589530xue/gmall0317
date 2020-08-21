package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
  * @author xjp
  */

// 需求：进行灵活查询
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    // 1.创建StreamingContext对象
    val conf: SparkConf = new SparkConf().setAppName("SalaDetailApp").setAppName("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 2.分别读取两个topic中的数据，生成对应的DS
    val orderInfoDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)
    val orderDetailDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL, ssc)

    //3.分别转化为(orderID,样例类)的kv格式，为后续join做准备(只有kv类型的DS才能join)
    val orderInfoKVDS: DStream[(String, OrderInfo)] = orderInfoDS.map(record => {
      val log: String = record.value()
      val info: OrderInfo = JSON.parseObject(log, classOf[OrderInfo])

      //a.给日期及小时字段重新赋值
      val create_time: String = info.create_time //2020-08-18 04:24:04
      val timeArr: Array[String] = create_time.split(" ")
      info.create_date = timeArr(0)
      info.create_hour = timeArr(1).split(":")(0)
      //b.给联系人手机号脱敏
      val telTuple: (String, String) = info.consignee_tel.splitAt(4)
      info.consignee_tel = telTuple._1 + "*******"

      (info.id, info)
    })

    val orderDetailKVDS: DStream[(String, OrderDetail)] = orderDetailDS.map(record => {
      val log: String = record.value()
      val detail: OrderDetail = JSON.parseObject(log, classOf[OrderDetail])
      // 此处的key不能使用detail.id,而是使用detail.order_id
      (detail.order_id, detail)
    })

    // 4.为了join后不丢失任何数据，对两个DS对象采用full join
    val fullJoinDS: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoKVDS.fullOuterJoin(orderDetailKVDS)

    // 5.分情况将fullJoinDS写出或写入Redis,此处使用mapPartition算子以减少redis连接的创建次数
    fullJoinDS.mapPartitions(iter => {

      //a.创建List对象盛放输出的SaleDetail对象
      val details = new ListBuffer[SaleDetail]

      //b.创建redis连接对象
      val client: Jedis = RedisUtil.getJedisClient

      //c.导入样例类对象转换为JSON的隐式
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //d.调用集合的foreach方法遍历iter中的数据
      iter.foreach { case (orderId, (infoOpt, detailOpt)) => {
        // 5.1 infoOpt有值的情况
        if (infoOpt.isDefined) {

          val infoOptVal: OrderInfo = infoOpt.get //此处易忘，需要获取Option对象的值
          // 5.1.1 detailOpt也同样有值的情况
          if (detailOpt.isDefined) {

            val detailOptVal: OrderDetail = detailOpt.get
            val sdObj = new SaleDetail(infoOptVal, detailOptVal)

            details += sdObj
          }

          // 5.1.2 detailOpt中没值时，则查看orderDetail的redis缓存(前期数据)中是否有匹配上的
          //       orderDetail的redis缓存是存储在Set结构中的(因为同一个key可能需要存多个信息)

          // 创建存储orderInfo的redis对象的key、存储orderDetail的redis对象的key
          val infoRedisKey = s"order_Info:$orderId"
          val detailRedisKey = s"order_detail:$orderId"

          val jsonObjs: util.Set[String] = client.smembers(detailRedisKey) //jsonObjs便为匹配上的详情数据

          jsonObjs.asScala.foreach(detailJasonObj => {

            val detailObj: OrderDetail = JSON.parseObject(detailJasonObj, classOf[OrderDetail])
            details += new SaleDetail(infoOptVal, detailObj)
          })
        }
      }
      }


      client.close()

      // 返回迭代器
      details.toIterator
    })


  }
}
