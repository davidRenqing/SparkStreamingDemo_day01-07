package cn.itcast.spark.day5

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 2016/5/21.
  */
object StreamingWordCount {

  def main(args: Array[String]) {

    //------------------6
    /*
    * 调用Log日志显示的信息．让Streaming输出时，把好多的不用的信息后屏蔽掉
    * */
    LoggerLevels.setStreamingLogLevels()


    //-------------1
    /*
    * 写spark程序得有一个SparkContext.
    * 那搞sparkStreaming 得有一个sparkStreamningContext
    * */
    //StreamingContext
    //这个Local[2]中的２，代表同时有两个Worker在跑程序？
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //这行程序是创建SparkStreamingContext
    //创建SparkStreamingContext之时，必须得有一个SparkContext
    //Seconds(5) 参数代表每多长时间产生一个批次
    val ssc = new StreamingContext(sc, Seconds(5))



    //-------------2
    /*
    * 接受数据的代码
    * socketTextStream　代表从主机＂172.16.0.11＂的8888端口上来读数据
    *具体的要查看socketTextStream这个函数的形参和返回值
    * 这个函数socketTextStream的返回值是DStream 类型数据
    * */
    //接收数据
    /*
    * 重要概念．这个ds是一系列的RDD.这个ds会每　Seconds(5)　秒，产生一个．这个时间间隔是可配置的
    * */
    val ds = ssc.socketTextStream("127.0.0.1", 8888)



    //--------------3
    /*
    * 对DStream类型的RDD，ds,进行算子操作
    * DStream 类型的RDD和普通的RDD的算子操作方法是相似的
    * */
    //DStream是一个特殊的RDD
    //hello tom hello jerry
    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)


    //---------------4
    /*
    * 将数据进行存储．将数据存储在Redix,或者MySql数据库
    * */
    //将数据存储在Redix中的步骤

    /*
    result.mapPartitions(it =>{

      val connection  //进行Redix驱动的连接
      it.map()    //在map函数中，将每一行数据存储到Redix数据库中
    })
    */

    //打印结果
    result.print()


    //-------------5
    /*
    * 把这个SparkStreaming 启动起来
    * */
    ssc.start()
    ssc.awaitTermination()   //等待着结束．以后你可以写一些指令，让这个Streaming停止运行．否则这个Streaming一直运行
  }
}