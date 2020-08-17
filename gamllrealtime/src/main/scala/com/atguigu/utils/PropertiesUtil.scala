package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
  * @author xjp
  */
// 作用：读取配置
object PropertiesUtil {
  def load(propertiesName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName) , "UTF-8"))
    prop
  }
}
