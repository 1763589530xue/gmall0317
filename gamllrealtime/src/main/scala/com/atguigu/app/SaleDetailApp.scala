
package com.atguigu.app

import java.time.LocalDate
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
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
    val conf: SparkConf = new SparkConf().setAppName("SalaDetailApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 2.分别读取两个topic中的数据，生成对应的DS
    val orderInfoDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)
    val orderDetailDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL, ssc)

    //3.分别转化为(orderID,样例类)的kv格式，为后续join做准备(只有kv类型的DS才能join)
    val orderInfoKVDS: DStream[(String, OrderInfo)] = orderInfoDS.map(record => {
      val log: String = record.value()
      println(log)
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

    // 5.分情况将fullJoinDS写出到details或写入Redis,此处使用mapPartition算子以减少redis连接的创建次数
    val noUserSaleDetail: DStream[SaleDetail] = fullJoinDS.mapPartitions(iter => {

      //a.创建List对象盛放输出的SaleDetail对象
      val details = new ListBuffer[SaleDetail]

      //b.创建redis连接对象
      val client: Jedis = RedisUtil.getJedisClient

      //c.导入样例类对象转换为JSON的隐式
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //d.调用集合的foreach方法遍历iter中的数据
      iter.foreach { case (orderId, (infoOpt, detailOpt)) => {
        // 创建存储orderInfo的redis对象的key、存储orderDetail的redis对象的key
        val infoRedisKey = s"order_Info:$orderId"
        val detailRedisKey = s"order_detail:$orderId"

        // 5.1 infoOpt有值的情况
        if (infoOpt.isDefined) {

          val infoObj: OrderInfo = infoOpt.get //此处易忘，需要获取Option对象的值
          // 5.1.1 detailOpt也同样有值的情况
          if (detailOpt.isDefined) {

            val detailOptVal: OrderDetail = detailOpt.get
            val sdObj = new SaleDetail(infoObj, detailOptVal)

            details += sdObj
          }

          // 5.1.2 detailOpt中没值时，则查看orderDetail的redis缓存(前期数据)中是否有匹配上的
          //       orderDetail的redis缓存是存储在Set结构中的(因为同一个key可能需要存多个信息)

          val jsonObjs: util.Set[String] = client.smembers(detailRedisKey) //jsonObjs便为匹配上的订单详情数据

          // 有可能没有值(即orderInfo来早了)，此时不会执行foreach算子
          jsonObjs.asScala.foreach(detailJasonObj => {

            //  set中可能存储了多条订单详情信息
            val detailObj: OrderDetail = JSON.parseObject(detailJasonObj, classOf[OrderDetail])
            details += new SaleDetail(infoObj, detailObj)

          })


          // 5.1.3 将orderInfo缓存到redis中，以供后续订单详情匹配使用(使用String结构存储即可)
          //       redis的value需要保存json格式数据，因此需要将infoOptVal转化为json格式
          //       JSON.toJSONString(infoOptVal)      编译报错
          val orderInfoJson: String = Serialization.write(infoObj) //因为采用String类型保存，因此必须将对象转化为String
          client.set(infoRedisKey, orderInfoJson)
          client.expire(infoRedisKey, 100) //每个infoRedisKey只存活100s

        } else {
          // 5.2 infoOpt没有值的情况(需要与redis中保存的orderInfo信息[前期数据]进行重新匹配，如果还是不能匹配则缓存至redis供后续匹配)

          // 5.2.1获取detailOpt中的数据
          val detailObj: OrderDetail = detailOpt.get

          // 5.2.2 如果按照infoRedisKey获取到了数据，便输出；如没获取到则将orderDetail信息保存至redis中
          val targetObj: String = client.get(infoRedisKey)

          if (targetObj != null) {
            //a. 如果在前期orderInfo的redis缓存中匹配到了数据，则直接写出
            details += new SaleDetail(JSON.parseObject(targetObj, classOf[OrderInfo]), detailObj)

          } else {
            //b.没有匹配成功则写入orderDetail的redis缓存中
            val orderDetailJson: String = Serialization.write(detailObj)
            client.set(detailRedisKey, orderDetailJson)
            client.expire(detailRedisKey, 100)
          }
        }
      }
      }
      client.close()
      // 返回迭代器
      details.toIterator
    })


    //6.将user信息与noUserSaleDetail进行关联匹配(在redis中完成的)
    //  此处不是最终的写库操作，以下操作得到的DS对象最终还要写到ES中，因此不能使用输出算子foreachRDD
    val UserSaleDetail: DStream[SaleDetail] = noUserSaleDetail.mapPartitions(iter => {
      val client: Jedis = RedisUtil.getJedisClient

      val details: Iterator[SaleDetail] = iter.map(saleDetailObj => {
        val searchKey = s"userInfo:${saleDetailObj.user_id}"
        val targetUserInfoObj: String = client.get(searchKey)

        val userInfoObj: UserInfo = JSON.parseObject(targetUserInfoObj, classOf[UserInfo])

        saleDetailObj.mergeUserInfo(userInfoObj)

        saleDetailObj
      })

      client.close()

      // 由于mergeUserInfo方法是直接对saleDetailObj进行属性修改的，因此返回iter即可
      details
    })


    //7. 打印测试
    UserSaleDetail.cache()
    UserSaleDetail.print(100)

    //8.将三个流合并后的数据输出到ES中(本质为写库操作)
    UserSaleDetail.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        // a.遍历生成要传入insertByBulk中的List对象
        //   文档名docId需要具备唯一性，此处采用订单详情id表示(易忘！)

        val detailIdToSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetailObj => {
          (saleDetailObj.order_detail_id, saleDetailObj)
        })

        //   indexName需要与kibana中模板的格式一致(此处采用前缀+当天日期形式表示)
        val today: String = LocalDate.now().toString
        val indexName: String = s"${GmallConstants.GMALL_ES_SALE_DETAIL_PRE}-$today"

        //获取连接,并将数据批量写入ES中(该方法中自动关闭了连接)
        MyEsUtil.insertByBulk(indexName, "_doc", detailIdToSaleDetailIter.toList)
      })
    })

    //9. 启动任务
    ssc.start()
    ssc.awaitTermination()

  }
}

