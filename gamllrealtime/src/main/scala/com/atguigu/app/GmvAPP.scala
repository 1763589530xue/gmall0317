package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
  * @author xjp
  */

//需求：计算每天/每小时的成交额
object GmvApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafakDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)

    // 1.将每条ConsumerRecord数据取出包装为样例类(因为record.value为json格式，因此需要使用JSON.parseObject方法)
    val changeDS: DStream[OrderInfo] = kafakDS.map(
      record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        // 2.1 对电话号码进行脱敏
        val tel: String = orderInfo.consignee_tel
        val tuple: (String, String) = tel.splitAt(4)
        orderInfo.consignee_tel = tuple._1 + "*******"

        // 2.2 对create_date和create_hour重新赋值
        val create_date: String = orderInfo.create_date
        val create_hour: String = orderInfo.create_hour
        val create_time: String = orderInfo.create_time

        orderInfo.create_date = create_time.split(" ")(0)
        orderInfo.create_hour = create_time.split(" ")(1).substring(0, 2)

        //3.返回orderInfo
        orderInfo
      }
    )

    // 4.将数据写到HBase(pheonix)中供显示页面取用(因为写库操作不需要返回值因此可以使用foreachRDD算子)
    changeDS.foreachRDD(
      rdd => rdd saveToPhoenix(
        "GMALL200317_ORDER_INFO",
        // 备注：如果下行不加getName，则输出的是全类名；
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("warehousehadoop102,wareshousehadoop103,wareshousehadoop104:2181")
      )
    )

    ssc.start()
    ssc.awaitTermination()

  }
}

