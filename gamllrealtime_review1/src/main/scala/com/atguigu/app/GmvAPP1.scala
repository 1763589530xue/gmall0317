package com.atguigu.app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderInfo, StartUpLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._

/**
  * @author xjp
  */

//交易额需求第一次复习(主要借助canal工具类)
object GmvAPP1 {
  def main(args: Array[String]): Unit = {
    // 1.由工具类创建DStream对象
    val conf: SparkConf = new SparkConf().setAppName("GmvAPP1").setMaster("local[*]")
    val scc = new StreamingContext(conf, Seconds(5))

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, scc)

    // 2.将json格式信息转化为样例类并修改补充字段(该步易忘！)

    val kafkaChangeDS: DStream[OrderInfo] = kafkaDS.map(
      record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //a.给日期及小时字段重新赋值
        val create_time: String = orderInfo.create_time //2020-08-18 04:24:04
        val timeArr: Array[String] = create_time.split(" ")
        orderInfo.create_date = timeArr(0)
        orderInfo.create_hour = timeArr(1).split(":")(0)

        //b.给联系人手机号脱敏
        val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
        orderInfo.consignee_tel = telTuple._1 + "*******"

        orderInfo
      }
    )

    kafkaChangeDS.cache()
    kafkaChangeDS.print()

    //3.将转换后的对象写入HBase中(使用输出算子foreachRDD)
    kafkaChangeDS.foreachRDD(
      rdd => rdd.saveToPhoenix("GMALL200317_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create,
        Some("warehousehadoop102,warehouehadoop103,warehouehadoop104:9092"))
    )

    //4.执行操作
    scc.start()
    scc.awaitTermination()
  }
}
