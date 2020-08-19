package com.atguigu.handler

import java.time.LocalDate
import java.util

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * @author xjp
  */

object Handler {

  // 1.进行批间去重(为了减少redis的连接数以及与redis的交互次数，优先使用广播变量方式)
  def filterByGroup(kafkaChangeDS: DStream[StartUpLog], scc: StreamingContext): DStream[StartUpLog] = {

    //  方式一：
    /*    kafkaChangeDS.transform(
          rdd => {
            // 25-32行是在Driver端执行的，每个周期只执行一次
            val client: Jedis = RedisUtil.getJedisClient
            //由于无时间获取源头，因此使用当天时间传入；
            val date: String = LocalDate.now().toString
            val strings: util.Set[String] = client.smembers(s"dau:${date}")
            // 广播变量是由sparkcontext对象调用方法生成的；
            val bcValues: Broadcast[util.Set[String]] = scc.sparkContext.broadcast(strings)
            client.close()

            //此处应该由rdd调用filter，而非kafkaChangeDS
            rdd.filter(log =>
              !bcValues.value.contains(log.mid)
            )
          })*/

    //方式二：
    kafkaChangeDS.mapPartitions(
      iter => {
        val client: Jedis = RedisUtil.getJedisClient
        val filterlogs: Iterator[StartUpLog] = iter.filter(
          log => {
            !client.sismember(s"dau:${log.logDate}", log.mid)
          }
        )
        client.close()

        //返回去重后的值
        filterlogs
      }
    )


    // 自己的错误方式：59-66只在driver端执行一次，本质上相当于68-82
    /*  val client: Jedis = RedisUtil.getJedisClient
        //由于无时间获取源头，因此使用当天时间传入；
        val date: String = LocalDate.now().toString
        val strings: util.Set[String] = client.smembers(s"dau:${date}")

        // 广播变量是由sparkcontext对象调用方法生成的；
        val bcValues: Broadcast[util.Set[String]] = scc.sparkContext.broadcast(strings)
        client.close()


        =>等效：
        val filterByGroupDS: DStream[StartUpLog] ={
        val client: Jedis = RedisUtil.getJedisClient
        //由于无时间获取源头，因此使用当天时间传入；
        val date: String = LocalDate.now().toString
        val strings: util.Set[String] = client.smembers(s"dau:${date}")

        // 广播变量是由sparkcontext对象调用方法生成的；
        val bcValues: Broadcast[util.Set[String]] = scc.sparkContext.broadcast(strings)
        client.close()

        kafkaChangeDS.filter(log =>
          !bcValues.value.contains(log.mid))
        }


        kafkaChangeDS.filter(log =>
          !bcValues.value.contains(log.mid)
        )*/


  }


  //  2.进行批内去重
  def filterByBatch(filterByBatchDS: DStream[StartUpLog]): DStream[StartUpLog] = {

    /* 自己第二次：由于filterByBatchDS中的数据可能跨天，因此需要先进行结构转换，将mid和logDate组合起来作为过滤条件!!!

    val filterByBatchDS: DStream[StartUpLog] = filterByBatchDS.transform(
       rdd => {
         // 进行分组，只取时间戳最早的一条数据
         val groupRdd: RDD[(String, Iterable[StartUpLog])] = rdd.groupBy(_.mid)
         val fistTsRdd: RDD[List[StartUpLog]] = groupRdd.map(_._2.toList.sortBy(_.ts).take(1))
         val resultRdd: RDD[StartUpLog] = fistTsRdd.flatMap(ele => ele)
         resultRdd
       }
     )
     filterByBatchDS*/

    val changeDS: DStream[(String, StartUpLog)] = filterByBatchDS.map(log => (s"${log.mid}-${log.logDate}", log))
    val groupDS: DStream[(String, Iterable[StartUpLog])] = changeDS.groupByKey()
    val sortDS: DStream[List[StartUpLog]] = groupDS.map(_._2.toList.sortBy(_.ts).take(1))

    val flatMapDS: DStream[StartUpLog] = sortDS.flatMap(ele => ele)
    flatMapDS
  }

  //  3.将两次过滤后的数据写入Redis
  def saveToRedis(filterByBatchDS: DStream[StartUpLog]) = {
    //  自己的错误写法：val client: Jedis = RedisUtil.getJedisClient实际上是在Driver端执行的，在rdd.foreachPartition中调用涉及跨节点传输
    /*  filterByBatchDS.foreachRDD(
        rdd => {
          // 每个Rdd只建立一个redis连接
          val client: Jedis = RedisUtil.getJedisClient

          // 下行不能使用map转换算子，而应该使用foreach输出算子
          rdd.foreachPartition {
            log => {
              val date: LocalDate = LocalDate.parse(log.ts.toString)
              val mid: String = log.mid
              client.sadd(s"dau:${date}", mid)
            }
          }
          client.close()
        }
      )*/


    filterByBatchDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            // 每个分区只建立一个redis连接,此时的client便是在executor端上创建的
            val client: Jedis = RedisUtil.getJedisClient
            iter.foreach(
              startlog => {
                val key = s"dau:${startlog.logDate}"
                client.sadd(key, startlog.mid)
              }
            )
            client.close()
          }
        )
      }
    )
  }
}
