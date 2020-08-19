package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.LocalDate

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import scala.util.control.Breaks._

/**
  * @author xjp
  */

//需求：恶意领取优惠券预警
object AlertApp {
  def main(args: Array[String]): Unit = {
    //  1.创建DStream对象
    val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    //  2.使用工具类获得kafka start数据流
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_EVENT, ssc)

    //  3.对获取的EVENT数据进行处理
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val changeDS: DStream[EventLog] = kafkaStream.map(record => {
      val str: String = record.value()
      val log: EventLog = JSON.parseObject(str, classOf[EventLog])

      val strings: Array[String] = sdf.format(log.ts).split(" ")
      log.logDate = strings(0)
      log.logHour = strings(1)

      log
    })


    // 4.使用开窗算子，以5min为窗口长度进行统一mid不同uid的过滤

    val windowDS: DStream[EventLog] = changeDS.window(Minutes(5))

    //没有对应的groupBy算子，只有转化为kv形式，使用groupByKey算子进行处理
    val groupDS: DStream[(String, Iterable[EventLog])] = windowDS.map(log => (log.mid, log)).groupByKey()

    val filterDS: DStream[CouponAlertInfo] = groupDS.map { case (mid, iter) => {
      val uidSet = new java.util.HashSet[String]()
      val itemIdSet = new java.util.HashSet[String]()
      val eventList = new java.util.ArrayList[String]()

      //a.设置判断是否浏览商品的flag
      var noClick = true

      breakable(
        iter.foreach(log => {
          val evid: String = log.evid
          eventList.add(evid) //所有的用户行为均要收集

          if ("coupon".equals(evid)) {
            itemIdSet.add(log.itemid)
            uidSet.add(log.uid)
          } else if ("clickItem".equals(evid)) {
            noClick = false
            break()
          }
        }
        ))

      //b.如果uid数大于等于3并且没有浏览商品，则列入警告范围(即将结果包装为样例类，具体为调用其apply方法)
      if (uidSet.size >= 3 && noClick) {
        CouponAlertInfo(mid, uidSet, itemIdSet, eventList, System.currentTimeMillis())
      } else {
        null
      }
    }
    }

    //5.过滤掉预警信息中为null的数据
    val newFilterDS: DStream[CouponAlertInfo] = filterDS.filter(info => info != null)

    newFilterDS.cache()   //因为调用了两次输出算子因此需要缓存起来
    newFilterDS.print()

    //6.将可能的预警信息写到ElasticSearch中，并按照分钟数进行文档存储，利用ES的幂等性进行去重
    newFilterDS.foreachRDD(rdd => { //写库操作需要的是输出算子，不能使用map
      rdd.foreachPartition(iter => {
        //a. 转换数据结构，  预警日志 ==> (docID,预警日志)
        val docIdToData: Iterator[(String, CouponAlertInfo)] = iter.map(info => {
          val minutes: Long = info.ts / 1000 / 60 //预警信息时间距离1970-01-01的分钟数
          (s"${info.mid}-${minutes}", info)
        })

        //b.使用自定义工具类将日志写入ES中
        val today: String = LocalDate.now().toString()
        MyEsUtil.insertByBulk(GmallConstants.GMALL_ES_ALERT_INFO_PRE + "_" + today, "_doc", docIdToData.toList)
      }
      )
    })

    //7.执行程序
    ssc.start()
    ssc.awaitTermination()
  }
}

