package cn.itcast.spark.day2

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/16.
  */
object UrlCount {

  def main(args: Array[String]) {

    //配置spark的信息
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)



    //rdd1将数据切分，元组中放的是（URL， 1）
    val rdd1 = sc.textFile("/home/hadoop01/sparkTest/itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)   //(URL,1)
    })


    //将(URL,1)按照URL作为key,将(URL,(1,1,1...))进行累加和
    val rdd2 = rdd1.reduceByKey(_+_)



    val rdd3 = rdd2.map(t => {
      val url = t._1  //把元组的第一个元素ＵＲＬ拿出来

      //1
      /*URL这是一个Java的一个类，你传进去一个url.
      * URL这个类自动帮你把url进行切割，然后你就可以使用URL类的方法，把url的各个字段提取出来了
      * */
      val host = new URL(url).getHost
      (host, url, t._2)   //封装的数据的形式(主机名,url,url访问的次数)
    })


    //3
    /*
    * rdd3.groupBy(_._1).collect()的执行结果，图１，所示
    * ArrayBuffer((net.itcast.cn,CompactBuffer((net.itcast.cn,http://net.itcast.cn/net/video.shtml,521)
    * 形成的ArrayBuffer中，会把 net.itcast.cn 提取出来，然后把rdd3中的hotst为net.itcast.cn的每一个元素放在一个数组CompactBuffer
    * 中，组成一个(net.itcast.cn,CompactBuffer（）)
    * */
    //print(rdd3.groupBy(_._1).collect().toBuffer)




    //4
    /*
    * groupBy算子会将rdd3按照host进行分组．如
    * mapValues的作用是，在不改变key值的作用下，同一组的value值进行操作
    * */

    /*
    * 将rdd3中每一组元组，都按照元组中url的总数来进行排序，截图，图２，所示
    * */

      //val rdd4 = rdd3.groupBy(_._1).mapValues(it =>{
        //it.toList.sortBy(_._3)
      //})

    /*
    * 我发现reverse是scala中的List的方法，不是ＲＤＤ的方法，我终于明白了．
    * it.toList.sortBy(_._3).reverse.take(3) 这行程序中，是先把分完组的rdd３在排完序之后，
    * it.toList 是把rdd3的每一个元素转换成数组，后边的sortBy,reverse 方法都是List的方法
    * 每一个List的存储和运行都是放在内存中的．
    * 如果rdd3中的源数据，太多的话，就会导致程序内存的不足，导致集群中的机器宕机，
    * 所以老师采用了第二种方法
    * */



//2
    //老师的代码，我在上面会把每一步的代码分开写．
    val rdd4 = rdd3.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(3) //先让rdd的元素按照第３个数，即url的次数进行排序．之后使用take函数，取出前３名
    })
    rdd4.saveAsTextFile("/home/hadoop01/sparkTest/out")


//    println(rdd4.collect().toBuffer)
    sc.stop()


  }
}