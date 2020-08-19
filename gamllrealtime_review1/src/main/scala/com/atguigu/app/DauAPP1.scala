package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.LocalDate

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.Handler
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

/**
  * @author xjp
  */

//总体原则：从kafka中获取的数据均为json格式，因此创建DS对象后首先要将json格式的数据转换为自定义样例类；
object DauAPP1 {
  def main(args: Array[String]): Unit = {
    // 1.由工具类创建DStream对象
    val conf: SparkConf = new SparkConf().setAppName("DauAPP1").setMaster("local[*]")
    val scc = new StreamingContext(conf, Seconds(5))

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_START, scc)

    // 2.将json格式信息转化为样例类并修改补充字段(该步易忘！)
    val sdf = new SimpleDateFormat()
    val kafkaChangeDS: DStream[StartUpLog] = kafkaDS.map(
      record => {
        val log: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        val str: String = sdf.format(log.ts, "yyyy-MM-dd HH")
        val strings: Array[String] = str.split(" ")
        log.logDate = strings(0)
        log.logHour = strings(1)
        log
      }
    )

    // 3.进行批间去重
    val filterByGroupDS: DStream[StartUpLog] = Handler.filterByGroup(kafkaChangeDS, scc)

    // 4.进行批内去重
    val filterByBatchDS: DStream[StartUpLog] = Handler.filterByBatch(filterByGroupDS)

    // 第二次写时忘记的步骤：将filterByBatchDS缓存起来(因为要使用两次)
    filterByBatchDS.cache()

    // 5.将新增数据写入redis(写库由于不需要返回，可以使用foreachRDD算子)
    Handler.saveToRedis(filterByBatchDS)

    // 6.将新增数据写入HBase(saveToPhoenix是RDD的算子，而非DS的算子)
    filterByBatchDS.foreachRDD(
      rdd => rdd.saveToPhoenix(
        "GMALL200317_DAU",
        classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase()),  //本行需要调用getName方法，并要将转换为大写以适应Pheonix；
        HBaseConfiguration.create(),
        Some("warehousehadoop102,warehousehadoop103,warehousehadoop104:2181")
      )
    )

    // 7.执行程序
    scc.start()
    scc.awaitTermination()
  }
}
