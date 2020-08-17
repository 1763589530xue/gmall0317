package com.atguigu.handler

import java.time.LocalDate
import java.util

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis


object DauHandler {

  // 1.进行批间的去重操作(为写redis库操作，不需要返回值，因此可以使用mapPatition算子)
  def filterByRedis(startLogDStream: DStream[StartUpLog], sparkContext: SparkContext) = {

    // 方式一：每个分区只创建一个redis连接，但是每条数据还要与redis交互一次；
    /*val value1: DStream[StartUpLog] = startLogDStream.mapPartitions(
      part => {
        val client: Jedis = RedisUtil.getJedisClient

        val logs: Iterator[StartUpLog] = part.filter(log => {
          !client.sismember(s"dau:${log.logDate}", log.mid)
        })
        client.close()
        // 将redis中不存在的数据返回
        logs
      }
    )
    value1*/

    // 方式二：使用广播变量方式，将每批次的redis中的set广播至Executor(一个采集周期中的set不发生变化)
    // 该方式优点：既减少了连接次数，也减少了与redis交互次数
    // transform和foreachRDD均是以RDD为单位进行处理，但是前者有返回值，而后者没有返回值

    val value2: DStream[StartUpLog] = startLogDStream.transform(rdd => {
      // 重点：a和b是在driver端执行，并非在executor端执行的；
      //a.获取redis中的set集合数据并广播，每个批次在Driver端执行一次
      val client: Jedis = RedisUtil.getJedisClient

      //b.获取set中的数据需要传入key，但是此处没有来源，只能使用系统中的当天时间(因为是实时处理，所以可以这么干)

      val today: LocalDate = LocalDate.now()
      val midSet: util.Set[String] = client.smembers(s"dau:${today}")
      val midSetBC: Broadcast[util.Set[String]] = sparkContext.broadcast(midSet)

      client.close()

      //c.在executor端使用广播变量进行去重(contains方法可以避免与redis的交互)
      rdd.filter(log => {
        !midSetBC.value.contains(log.mid)
      })
    })
    value2
  }


  //  2.对上次对比中redis中不存在的元素进行二次去重
  def filterByGroup(filterByRedisDS: DStream[StartUpLog]) = {
    // a.由于可能有多天数据，不能直接按照mid进行分组，需要先进行格式转化(加上key)
    val kvRdd: DStream[(String, StartUpLog)] = filterByRedisDS.map(startUpLog => (s"${startUpLog.mid}-${startUpLog.logDate}", startUpLog))

    val groupRdd: DStream[(String, Iterable[StartUpLog])] = kvRdd.groupByKey()

    // b.对于重复key的value值只取时间戳最小的信息
    val sortRdd: DStream[List[StartUpLog]] = groupRdd.map(_._2.toList.sortBy(_.ts).take(1))

    sortRdd.flatMap(ele => ele)
  }


  // 3.保存两次去重后的数据至Redis[此处同时涉及DStream、RDD和Scala中的集合，很多算子有重合部分，需要理清思路！]
  def saveToRedis(filterByGroupDS: DStream[StartUpLog]) = {
    //思路：由于写库操作不需要返回值，因此对于DStream可以使用foreachRDD算子(实则只有一个RDD吧？？？)
    //     为减少redis的连接数，因此对于RDD可以使用foreachPartition代替foreach
    filterByGroupDS.foreachRDD {
      rdd => {
        rdd.foreachPartition {iter => {
          val client: Jedis = RedisUtil.getJedisClient
          //iter.map(log => client.sadd(s"dau:${log.logDate}", log.mid))   使用map应该也可以(待验证)
          iter.foreach(log => client.sadd(s"dau:${log.logDate}", log.mid))
          client.close()
        }
        }
      }
    }
  }
}
