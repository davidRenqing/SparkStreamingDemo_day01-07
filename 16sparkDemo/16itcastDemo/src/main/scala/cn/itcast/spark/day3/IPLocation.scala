//package cn.itcast.spark.day3
//
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * Created by root on 2016/5/18.
//  */
//object IPLocation {
//
//
//  //ip转换成十进制
//  def ip2Long(ip: String): Long = {
//    val fragments = ip.split("[.]")
//    var ipNum = 0L
//    for (i <- 0 until fragments.length){
//      ipNum =  fragments(i).toLong | ipNum << 8L
//    }
//    ipNum
//  }
//
//
//
//  //--------------4
//  //使用二分法来进行ip和分区的查询
//  /*
//  * 这个方法返回，当下的ip在lines　数组中的索引
//  * */
//  def binarySearch(lines: Array[(String, String, String)], ip: Long) : Int = {
//    var low = 0
//    var high = lines.length - 1
//    while (low <= high) {
//      val middle = (low + high) / 2
//      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
//        return middle
//      if (ip < lines(middle)._1.toLong)
//        high = middle - 1
//      else {
//        low = middle + 1
//      }
//    }
//    -1
//  }
//
//  def main(args: Array[String]) {
//
//
//    val conf = new SparkConf().setMaster("local[2]").setAppName("IpLocation")
//    val sc = new SparkContext(conf)
//
//
//    //--------------1
//    /*
//    * 将ip.txt文件中的内容读到一个RDD中
//    * */
//    val ipRulesRdd = sc.textFile("/home/hadoop01/sparkTest/ip.txt").map(line =>{
//      val fields = line.split("\\|")
//      val start_num = fields(2) //读出ＲＤＤ的每一条内容的IP的初始值
//      val end_num = fields(3)   //ＩＰ区间的终止值
//      val province = fields(6)  //这个IP区间对应的地区的信息
//      (start_num, end_num, province)
//    })
//
//
//    //-------------2
//    /*
//    * 现在ip.txt的内容，被转换成了ipRulesArrary．数组类型的数据.此时ip.txt的全部内容都只是存储在发布作业的那一台Driver上面
//    * 这台Driver计算机，现在要将ipRulesArrar当中的所有的数据广播到Spark集群中的参与当前作业执行的所有的Worker中
//    * */
//    //全部的ip映射规则
//    val ipRulesArrary = ipRulesRdd.collect()
//
//    //广播规则
//    val ipRulesBroadcast = sc.broadcast(ipRulesArrary)
//
//
//
//    //------------3
//    /*
//    * 20090121000132.394251.http.format就是要处理的网关的源数据
//    * 将要处理的源数据从文件中读取到RDD当中
//    * */
//    //加载要处理的数据
//    val ipsRDD = sc.textFile("/home/hadoop01/sparkTest/20090121000132.394251.http.format").map(line => {
//      val fields = line.split("\\|")
//      fields(1)
//    })
//
//    //将ipsRDD中的每一条记录的ip取出来．将ip转换成十进制，之后调用binarySearch找
//    val result = ipsRDD.map(ip => {
//      val ipNum = ip2Long(ip) //将ipsRDD中的每一条记录的ip取出来．将ip转换成十进制
//      val index = binarySearch(ipRulesBroadcast.value, ipNum) //调用binarySearch找到每一个ip对应在ipRulesBroadcast中的索引值
//      val info = ipRulesBroadcast.value(index)         //取得ip对应的地区的信息
//      (ip,info._3)  //将info信息作为RDDresult的新的元素．这样RDDresult中存储的就是由info组成的RDD
//    })
//
//   // println(result.collect().toBuffer)
//
//    result.saveAsTextFile("/home/hadoop01/sparkTest/out")
//
//    sc.stop()
//
//
//
//  }
//}



package cn.itcast.spark.day3

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/18.
  */
object IPLocation {


  //----------6
  /*
  * 定义一个往数据库中写数据的函数
  * */
  val data2MySQL = (iterator: Iterator[(String, Int)]) => {

    //我感觉和ｊａｖａ当中的创建数据库的流程一样．不过scala中的数据类型都用var 代替就可以了．
    var conn: Connection = null
    var ps : PreparedStatement = null
    val sql = "INSERT INTO location_info (location, counts, accesse_date) VALUES (?, ?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456")


      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(String, String, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("IpLocation")
    val sc = new SparkContext(conf)

    val ipRulesRdd = sc.textFile("/home/hadoop01/sparkTest/ip.txt").map(line =>{
      val fields = line.split("\\|")
      val start_num = fields(2)
      val end_num = fields(3)
      val province = fields(6)
      (start_num, end_num, province)
    })
    //全部的ip映射规则
    val ipRulesArrary = ipRulesRdd.collect()

    //广播规则
    val ipRulesBroadcast = sc.broadcast(ipRulesArrary)

    //加载要处理的数据
    val ipsRDD = sc.textFile("/home/hadoop01/sparkTest/20090121000132.394251.http.format").map(line => {
      val fields = line.split("\\|")
      fields(1)
    })


    //--------------4
    /*
    * RDD　result　中存储的每一个元素的内容是(ip的起始Num， ip的结束Num，省份名)
    *
    * */
    val result = ipsRDD.map(ip => {
      val ipNum = ip2Long(ip)
      val index = binarySearch(ipRulesBroadcast.value, ipNum)
      val info = ipRulesBroadcast.value(index)
      //(ip的起始Num， ip的结束Num，省份名)
      info
    }).map(t => (t._3, 1)).reduceByKey(_+_) //这行代码的作用是，把result 中的省名进行累加



   // print(result.collect.toBuffer) //ArrayBuffer((陕西,1824), (河北,383), (云南,126), (重庆,868), (北京,1535))



    //---------------5
    //将数据处理的结果存储到MySQl 当中
    //向MySQL写入数据
    //result.foreachPartition和result.foreach 的区别就是　foreach算子没写一个数据就会创建一次链接．而foreachPartition，按照分区操作
    //每将一个分区的数据写到数据库，只需要建立一次链接就可以了

    /*
    * forEach(func) 的做法是，从rdd中拿出一条，就建立一次数据库的链接．这样rdd有多少个元素，他就建立多少次数据库的链接
    * 可是forEachPartiton(func) 按照分区操作．这样func 中的操作每次只建立一次数据库的链接，就可以把这个分区中的所有的元素都写进数据库当中去．
    * */
    result.foreachPartition(data2MySQL(_))


    //println(result.collect().toBuffer)

    sc.stop()



  }
}
