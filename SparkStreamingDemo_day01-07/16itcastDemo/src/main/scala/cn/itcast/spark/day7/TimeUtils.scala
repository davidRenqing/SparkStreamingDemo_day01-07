package cn.itcast.spark.day7

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by root on 2016/5/23.
  * 这个类的作用是对日期来进行操作
  * 这个TimeUtils也是一个单例模式
  */
object TimeUtils {

  //-----------------------11
  /*
  * 创建时间的格式
  * */
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calendar = Calendar.getInstance()


  def apply(time: String) = {
    calendar.setTime(simpleDateFormat.parse(time))
    calendar.getTimeInMillis
  }


  /*
  * 从当前时间中减去amount天．这样就可以定位到计算n天前的数据了
  * */
  def getCertainDayTime(amount: Int): Long ={
    calendar.add(Calendar.DATE, amount)
    val time = calendar.getTimeInMillis

    //计算时间
    calendar.add(Calendar.DATE, -amount)
    time
  }



}
