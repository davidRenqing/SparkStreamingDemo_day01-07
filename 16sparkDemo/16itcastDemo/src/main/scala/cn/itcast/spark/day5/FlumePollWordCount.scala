package cn.itcast.spark.day5

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumePollWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlumePollWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))



    //-------------1
    //从flume中拉取数据(flume的地址)
    //172.16.0.11是Flume主机．代表SparkStream从Flume主机的这个端口下去拉取数据
    //这个address是一个数组，我可以把多个Flume主机的IP和端口号放在这个address数组当中．
    // 这样我的SparkStreaming就可以从多个Flume主机中去拉取数据了
    val address = Seq(new InetSocketAddress("172.16.0.11", 8888))


    //----------------2
    //图４有３个jar包　apache-flume-1.6.0-bin.tar.gz　commons-lang3-3.3.2.jar　
    // spark-streaming-flume-sink_2.10-1.6.1.jar　你要把这３个jar包都放在Flume的安装包中
    // 这３个安装包可以在，图４，所示的网站中下载下来
    //定义一个Flume对象．这样利用createPollingStream函数，SparkStreaming就会通过Flume从这个Flume的地址中把数据
    //拉取下来
    val flumeStream = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK)
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_,1))
    val results = words.reduceByKey(_+_)



    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}