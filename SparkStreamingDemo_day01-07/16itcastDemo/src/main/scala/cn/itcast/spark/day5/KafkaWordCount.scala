package cn.itcast.spark.day5

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 2016/5/21.
  */
object KafkaWordCount {


  //==========================3
  /*
  * updateFunc这个函数的作用是累加以前的数据，求和
  * */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
  }


  def main(args: Array[String]) {

    //=======================6
    //LoggerLevels 这个函数是老师自己写的，这样等Kafka在显示屏上显示数据时，就会显示很少的系统数据
    LoggerLevels.setStreamingLogLevels()

    //使用args来接受zkQuorum，group这些参数.也就是说你把程序打成jar包之后，要把这些参数传进去才可以
    val Array(zkQuorum, group, topics, numThreads) = args

    //=======================1
    /*
    * 首先要创造一个Spark的对象
    * */
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))  //设置每5s,产生一个SparkStreaming
    ssc.checkpoint("/home/hadoop/checkpoint")  //设置RDD当中的数据的checkpoint的文件夹目录


    //"alog-2016-04-16,alog-2016-04-17,alog-2016-04-18"
    //"Array((alog-2016-04-16, 2), (alog-2016-04-17, 2), (alog-2016-04-18, 2))"

    //一个sparkStreaming可能从Kafka中的多个topic当中读取数据。
    // 这个topicMap就是存储sparkStreaming从哪些topic中读取数据
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap



    //==============================2
    /*
    * 创建一个从Kafka中读取数据的流
    * 从Kafka中读取数据的方式有两种，一种是KafkaUtils.createStream
    * 另外一种是只连的方式 KafkaUtils.createDirectStream()
    * */

    /*
    * createScream中的形参的类型
    * def createStream(
      ssc: StreamingContext,
      zkQuorum: String,   zookeeper的一些配置
      groupId: String,    spark读取的kafka中的组的名字
      topics: Map[String, Int],   制定topic
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    )
    * */
    //date就是sparkStreaming从Kafka的一个topic当中拿出的数据
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)


    //===================================3
    //从Kafka当中拿到的数据是<key,value>类型的数据。因此要使用data.map(_.2)才可以把存储在Kafka当中的log信息拿出来
    //并且对拿出来的log信息按照“ ”，空格来进行切割
    val words = data.map(_._2).flatMap(_.split(" "))


    //==================================4
    /*
    * 对stream当中的每个单词变成<key，1> 的形式
    * updateStateByKey 把key相同的value按照updateFunc函数来进行操作
    * */
    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)


    //===================================5
    /*
    * 开始这个SparkStreaming的数据流
    * */
    ssc.start()
    ssc.awaitTermination()
  }
}