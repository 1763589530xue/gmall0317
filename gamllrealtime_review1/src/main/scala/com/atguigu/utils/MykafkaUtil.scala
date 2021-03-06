package com.atguigu.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

//作用：获得Destream对象；

object MyKafkaUtil {

  //1.创建配置信息对象
  private val properties: Properties = PropertiesUtil.load("config.properties")

  //2.用于初始化链接到集群的地址
  val broker_list: String = properties.getProperty("kafka.broker.list")
  private val group_id: String = properties.getProperty("kafka.group.id")

  //3.kafka消费者配置
  val kafkaParam = Map[String, Object](
    //  下行一般通过调用ConsumerConfig属性的方法来获取(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)

    "bootstrap.servers" -> broker_list,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //消费者组
    "group.id" -> group_id,
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    // ConsumerConfig.AUTO_OFFSET_RESET_CONFIG->"latest"
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  // 创建DStream，返回接收到的输入数据
  // LocationStrategies：根据给定的主题和集群地址创建consumer
  // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
  // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
  // ConsumerStrategies.Subscribe：订阅一系列主题
  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      // 下行中的[String, String]分别表示k和v类型(自己写得时候容易忘记！！！)
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))

    dStream
  }


  /*   第一遍重写：
      val kafkaParam1 = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
      ConsumerConfig.GROUP_ID_CONFIG -> group_id,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest"
    )

    def getKafkaStream1(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
      val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam1))

      value
    }*/
}
