package cn.itcast.spark.day3

import java.net.URL

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by root on 2016/5/18.
  */
/**
  * 这个例子在我的spark系统上运行成功．
  */
object UrlCountPartition {

  def main(args: Array[String]) {


    //写一些spark程序的配置信息
    val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]") //制定本地运行
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
      (host, (url, t._2))
    })


    //-----------1.3
    /*
    * 取出rdd3一共有多少个分区
    * rdd3.map(_._1).distinct()是统计一共有多少种host
    * */
    val ints = rdd3.map(_._1).distinct().collect()

    val hostParitioner = new HostParitioner(ints)




    //----------4
    /*
    * rdd3调用partitionBy算子，把我自定义的hostParitioner类传进去
    * 这就代表在分区的时候使用的是我自己的分区函数
    * */
    //    val rdd4 = rdd3.partitionBy(new HashPartitioner(ints.length))

    /*
    * 下边的程序可以可以等价成若干个步骤
    * val rdd4 = rdd3.partitionBy(hostParitioner)
    * val rdd5 = rdd4.mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })
    * */
    val rdd4 = rdd3.partitionBy(hostParitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator //对分区后的RDD进行操作，先排序，在去除前２名
    })

    rdd4.saveAsTextFile("/home/hadoop01/sparkTest/out4")


    //println(rdd4.collect().toBuffer)
    sc.stop()

  }
}


//------------1
/*
* 重写分区方法
* */

/**
  * 决定了数据到哪个分区里面
  * @param ins
  */
class HostParitioner(ins: Array[String]) extends Partitioner {

  //-----------2
  /*
  * parMap中存储(host,分区)
  * 如(java,1) (php,2),(net,3)
  * */
  val parMap = new mutable.HashMap[String, Int]()
  var count = 0
  for(i <- ins){
    parMap += (i -> count) //(i ->count) 是元组类型．(host,这个host的分区)
    count += 1
  }

  //--------1.1
  /*
  * 得到分区的数量．这个分区的数量就是你的规则库的数量，即你有几个学院．
  * 因为真实的业务要求分区的情况，就是你有几个学院，你就分几个区就可以了
  * */

  //ins的作用是统计有多少个host值，看1.3

  override def numPartitions: Int = ins.length


  //-----------1.2
  //这个getPartition　方法的作用是你传进去一个RDD的key,然后这个函数就会决定要把你这个key,放在什么哪一个分区当中
  override def getPartition(key: Any): Int = {


    //----------3
    /*
    * 这个parMap当中存储的就是各个学院的分区号
    * 你把key.toString传给parMap，就会得到分区号．
    * 比如key是"java",parMap中有元组(java,0) 那么parMap的返回值是１
    * 如果你传入的key在parMap当中没有，那么就会传回分区０
    * */
    parMap.getOrElse(key.toString, 0)
  }
}




//
//package cn.itcast.spark.day3
//
//import java.net.URL
//
//import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
//
//import scala.collection.mutable
//
///**
//  * Created by root on 2016/5/18.
//  */
//object UrlCountPartition {
//
//  def main(args: Array[String]) {
//
//    val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//
//    //rdd1将数据切分，元组中放的是（URL， 1）
//    val rdd1 = sc.textFile("c://itcast.log").map(line => {
//      val f = line.split("\t")
//      (f(1), 1)
//    })
//    val rdd2 = rdd1.reduceByKey(_ + _)
//
//    val rdd3 = rdd2.map(t => {
//      val url = t._1
//      val host = new URL(url).getHost
//      (host, (url, t._2))
//    }).cache()//cache会将数据缓存到内存当中，cache是一个Transformation，lazy
//
//    val ints = rdd3.map(_._1).distinct().collect()
//
//    val hostParitioner = new HostParitioner(ints)
//
//    //    val rdd4 = rdd3.partitionBy(new HashPartitioner(ints.length))
//
//    val rdd4 = rdd3.partitionBy(hostParitioner).mapPartitions(it => {
//      it.toList.sortBy(_._2._2).reverse.take(2).iterator
//    })
//
//    rdd4.saveAsTextFile("c://out4")
//
//
//    //println(rdd4.collect().toBuffer)
//    sc.stop()
//
//  }
//}
//
///**
//  * 决定了数据到哪个分区里面
//  * @param ins
//  */
//class HostParitioner(ins: Array[String]) extends Partitioner {
//
//  val parMap = new mutable.HashMap[String, Int]()
//  var count = 0
//  for(i <- ins){
//    parMap += (i -> count)
//    count += 1
//  }
//
//  override def numPartitions: Int = ins.length
//
//  override def getPartition(key: Any): Int = {
//    parMap.getOrElse(key.toString, 0)
//  }
//}