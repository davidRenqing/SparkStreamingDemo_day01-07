package cn.itcast.spark.day2

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/16.
  */
object AdvUrlCount {

  def main(args: Array[String]) {


    //------------3
    //从数据库中加载规则
    val arr = Array("java.itcast.cn", "php.itcast.cn", "net.itcast.cn")


    //配置Spark的运行信息
    val conf = new SparkConf().setAppName("AdvUrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)



    //rdd1将数据切分，元组中放的是（URL， 1）
    val rdd1 = sc.textFile("/home/hadoop01/sparkTest/itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })

    val rdd2 = rdd1.reduceByKey(_ + _)

    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2)
    })


    //－－－－－－－－－－－1
    /*
    * 老师打算将rdd3中的同一个url的数据提取出来．
    * 分别取出每一个ｕｒｌ的前３名
    * */
    //println(rdd3.collect().toBuffer)

    //val rddjava = rdd3.filter(_._1 == "java.itcast.cn") //这行代码的执行结果，图３所示
    //    val sortdjava = rddjava.sortBy(_._3, false).take(3)
    //    val rddphp = rdd3.filter(_._1 == "php.itcast.cn")




    //--------------2
    /*
    * arr是上面定义的存储url数组的列表．
    * 这个列表，你可以从数据库中读取．之后把他放在数组中
    * 你用一个for循环，不断的遍历这个arr数组，这样就可以把每一个学院都遍历到了．
    * */

    /**
      *
      */
    for (ins <- arr) {
      val rdd = rdd3.filter(_._1 == ins)  //先把每一个学院的url统一筛选出来．就像步骤１中显示的那样
      val result= rdd.sortBy(_._3, false).take(3)  //按照usr


      //------------5
      /*
      * 相比于第一个程序UrlCount.scala 中的代码例子，这个例子使用rdd中的sortBy算子．因为ＲＤＤ是可以放在
      * 磁盘中的，这样如果处理的数据太多，ＲＤＤ可以把数据存储在磁盘上，而不是UrlCount的例子，List必须存储在
      * 内存中．这样，内存就不用花费空间，来存储RDD了．而是节省更多的空间，让内存有更多的空间专注于对于RDD的运算
      * 这样Spark集群，宕机的概率就会小很多了
      * */



      //-------------4
      /*
      * 将每一个url处理的前３名的结果，存储在数据库中．
      * 或者你可以定义一个全局的变量．等所有的url处理完之后，再把这个中间的结果，统一写在一个文件中．
      * */
      //通过JDBC向数据库中存储数据
      //id，学院，URL，次数， 访问日期
      println(result.toBuffer)
    }



    //println(sortdjava.toBuffer)
    sc.stop()

  }
}