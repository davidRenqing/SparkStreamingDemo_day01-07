package cn.itcast.spark.day5

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ZX on 2015/6/22.
  */
object FlumePushWordCount {

  def main(args: Array[String]) {

    //-------------1
    val host = args(0)   //指定Flume要把Flume收集到的数据传送给的Spark集群的Mater的IP地址
    val port = args(1).toInt  //指定Flume从该Master的哪个端口把数据发给这台Master主机
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("FlumeWordCount")//.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))


    //-------------------2
    /*
    * 创建一个Flume对象，让Flume把收集到的数据一直发送给 host:port
    * 这个函数FlumeUtils.createStream同时也需要一个StreamingContext对象ssc
    * 23:10 老师往Maven库中导入spark-streaming-flume.jar开发包
    * */
    //推送方式: flume向spark发送数据
    val flumeStream = FlumeUtils.createStream(ssc, host, port)



    //-----------3
    //flume中的数据通过event.getBody(),将event.getBody()转换成String类型的数据，
    // 才能拿到真正的Flume收集的内容.之后按照"空格"来切分数据
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_, 1))

    val results = words.reduceByKey(_ + _)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}