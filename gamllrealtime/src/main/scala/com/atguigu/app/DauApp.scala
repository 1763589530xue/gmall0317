package com.atguigu.app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingConf, StreamingContext}
import org.apache.phoenix.spark._


/**
  * @author xjp
  *         2020-08-15 19:57
  */

//需求：求日活实时信息
//来源：start启动日志
//思路：为了去重，需要将每批次的数据暂存在redis中，并使用set类型数据结构进行存储

object DauApp {

  def main(args: Array[String]): Unit = {
    //  1.创建DStream对象
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    //  2.使用工具类获得kafka start数据流
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_START, ssc)

    //  3.解析json信息，并转化为样例类(此处ConsumRecorder的key为null，我们只使用其value值)
    //    SimpleDateFormat对象在此处创建，可以避免再map中多次创建
    val format = new SimpleDateFormat("yyyy-MM-dd HH")

    val startLogDStream: DStream[StartUpLog] = kafkaStream.map(ele => {
      // a.获取ConsumerRecorder的value值(即json信息)
      val value: String = ele.value()

      //b.将json信息转化为样例类，但其中有logDate和logHour两个属性默认为null
      val log: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

      //c.重新对以上两个属性进行赋值
      val ts: Long = log.ts
      val dateStr: String = format.format(ts)
      val strings: Array[String] = dateStr.split(" ")
      log.logDate = strings(0)
      log.logHour = strings(1)
      //此处需要返回值(易忘！)
      log
    })

    //    startLogDStream.cache()
    //    startLogDStream.count().print()

    //  4.进行批次间去重(最新一批数据与redis数据之间去重，返回redis中不存在的数据)
    val filterByRedisDS: DStream[StartUpLog] = DauHandler.filterByRedis(startLogDStream, ssc.sparkContext)

    //    filterByRedisDS.cache()
    //    filterByRedisDS.count().print()

    //  5.进行批次内去重(4中redis中不存在的元素作为一批次进一步进行去重[因为4中是逐条对比的可能仍有重复])
    val filterByGroupDS: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDS)


    //    filterByGroupDS.cache()
    //    filterByGroupDS.count().print()

    //6.将两次去重后的数据写到redis中(由于是累加，因此每次只写新增数据即可)
    DauHandler.saveToRedis(filterByGroupDS)


    //  7.将两次去重后的数据写入HBase中
    filterByGroupDS.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL200317_DAU",
        // 使用反射方式获取所有字段，否则要写成Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS")样式
        classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("warehousehadoop102,warehousehadoop103,warehousehadoop104:2181"))
    })

    //  8.启动任务
    ssc.start()
    ssc.awaitTermination()
  }

}
