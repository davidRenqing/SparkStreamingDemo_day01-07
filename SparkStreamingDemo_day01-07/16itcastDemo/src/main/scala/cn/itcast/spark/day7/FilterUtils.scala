package cn.itcast.spark.day7

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by root on 2016/5/24.
  */
object FilterUtils {


  //--------------------------7
  /*
  * SimpleDateFormat的使用涉及到了线程不安全的问题
  * 而，FastDateFormat这个类是线程安全的
  * FastDateFormat对象org.apache.commons.lang3.time
  * */
  //在getInstance方法中写上时间的格式
  val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss") //E代表星期，HH代表小时，mm代表分钟,ss代表秒


  //-----------------------6
  /*
  * 创建一个filterByTime方法
  * 因为这个filterByTime方法在程序执行的时候会同时被多个线程调用．
  * 被多个线程调用就涉及到了线程同步的问题
  * */

  def filterByTime(fields: Array[String], startTime: Long, endTime: Long): Boolean = {
    val time = fields(1) //接受被切割完的每条Log的第1个字段，这个字段是时间
    //﻿1|2016年2月1日,星期一,10:01:08|10.51.4.168|李明克星|法师|男|1|0|0/800000000  fields(1)是 2016年2月1日,10:01:08|10.51.4.168
    //为什么fields是一个string类型的数组呢？因为源数据被"|"切割之后，像1,2016年2月1日,星期一,10:01:08这些字段都会是stirng类型的数据
    //这些数据被装进了一个string数组当中


    //--------------------------------------8
    //使用dateFormat类对time进行格式的转换．然后使用getTime 得到时间

    /*
    注意dateFormat是java的jar包，而Scala可以无缝的兼容java程序．
    * 这行程序在java中的样子是
    * Date logTime = dateFormat.parse(time).getTime()
    * 但是在Scala中数据类型由Scala自己进行判断所有有Date的位置写成val就可以
    * getTime()这个方法因为没有形参，所以将getTime的方法去掉()就可以了
    * */
    val logTime = dateFormat.parse(time).getTime


    //----------------------------------9
    //如果这个logTime是介于startTime和endTime之间的时间，就返回true
    //return logTime >= startTime && logTime < endTime

    logTime >= startTime && logTime < endTime
  }

  def filterByType(fields: Array[String], evenType: String) : Boolean = {
    val _type = fields(0)
    evenType == _type
  }

  def filterByTypes(fields: Array[String], eventTypes: String*): Boolean = {
    val _type = fields(0)
    for(et <- eventTypes){
      if(_type == et)
        return true
    }
    false
  }

  def filterByTypeAndTime(fields: Array[String], eventType: String, beginTime: Long, endTime: Long): Boolean = {
    val _type = fields(0)
    val _time = fields(1)
    val logTime = dateFormat.parse(_time).getTime

    //-------------------------------17
    /*
    * 用于计算时间
    * */
    eventType == _type && logTime >= beginTime && logTime < endTime
  }
}
