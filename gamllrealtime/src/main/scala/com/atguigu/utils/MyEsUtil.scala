package com.atguigu.utils

import java.util.Objects

import com.atguigu.bean.CouponAlertInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

/**
  * @author xjp
  */

//将数据写入ES的工具类
object MyEsUtil {
  private val ES_HOST = "http://warehousehadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _


  // 1.获取客户端
  def getCient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  //  2.关闭客户端
  def close(client: JestClient) = {
    if (!Objects.isNull(client))
      try {
        client.shutdownClient()
      } catch {
        case e: Exception => e.printStackTrace()
      }
  }

  //  3.建立连接的方法
  def build() = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(10000)
      .build)
  }

  //  4.批量插入数据到ES
  def insertByBulk(indexName: String, typeName: String, list: List[(String, CouponAlertInfo)]): Unit = {

    //  a.集合判空
    if (list.size != 0) {
      // b. 获取连接
      val client: JestClient = getCient

      //c.创建Bilk.Builder对象(此处不能直接调用bulid方法，因为后续要做批量插入操作)
      //  Index相当于数据库，typeName相当于表，docId相当于存储文档名
      val builder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType(typeName)

      // d.遍历list,创建Index对象，并放入Bulk.Builder对象
      list.foreach {
        case (docId, data) =>
          // e.为每条数据创建Index对象
          val index: Index = new Index.Builder(data).id(docId).build()
          builder.addAction(index)
      }

      // f.Bulk.Builder创建Bulk对象
      val bulk: Bulk = builder.build()

      // g.执行写入操作
      client.execute(bulk)

      // h.释放资源
      close(client)
    }
  }
}
