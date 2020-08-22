package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @author xjp
  */

//需求：将User的插入/更新信息保存到reids上
object SaveUserInfoToRedisApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SaveUserInfoToRedisApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_USER_INFO, ssc)

    kafkaDS.foreachRDD(rdd => {
      // 此处由于是写库操作，并且没有返回值，因此不能使用mapPartition算子，而是使用行动算子foreachPartition；
      rdd.foreachPartition { iter =>
        val client: Jedis = RedisUtil.getJedisClient

        iter.foreach(record => {
          val userInfoVal: String = record.value()

          // userInfo信息在redis中缓存为String格式，其中的key要用到UserInfo对象的id，因此此处需要转换为样例类
          val userInfo: UserInfo = JSON.parseObject(userInfoVal, classOf[UserInfo])
          val userRedisKey: String = s"userInfo:${userInfo.id}"

          // 将每个record作为value保存在String结构中
          client.set(userRedisKey, userInfoVal)
        })
        client.close()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
