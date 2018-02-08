package cn.itcast.spark.day7

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/24.
  */
object ScannPlugins {

  def main(args: Array[String]) {
    //==================================1
    /*
    * zkQuorm 是指zookeeper的集群
    * group 让SparkStreaming从Kafka的哪个分组中提取数据
    * */
    val Array(zkQuorum, group, topics, numThreads) = Array("node-1.itcast.cn:2181,node-2.itcast.cn:2181,node-3.itcast.cn:2181", "g0", "gamelogs", "1")

    //设置数据的格式
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    //配置Spark的conf配置文件
    val conf = new SparkConf().setAppName("ScannPlugins").setMaster("local[4]")

    //？？？没弄明白这是什么含义
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    //Milliseconds(10000)是指每10000毫秒就会将从RDD提取出来的数据转换成一个RDD交给SparkStreaming来进行处理
    val ssc = new StreamingContext(sc, Milliseconds(10000))



    //=================================2
    /*
    * 创建一个目录
    * 记录价格RDD使用checkPoint算子之后将数据保存在哪一个目录当中
    * */
    sc.setCheckpointDir("c://ck0")


    //=====================================5
    /*
    * 20:00~30:00这一段老师对Kafka的参数进行配置，我没有看懂！
    * */
    //topicMap 是指，SparkStreaming可能从Kafka的多个topic中读取数据。topicMap就是Kafka读取数据的topic数组
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, //因为我使用的是Receiver的连接方式，所有这里要配置Zookeeper的服务器
      "group.id" -> group, //从哪个Kafka的哪个group中把数据提取出来
      "auto.offset.reset" -> "smallest" //从Log日志的数据最开头的位置来提取数据
    )


    //=================================3
    /*
    * 使用Kafka的creatStream来得到一个流.Kafka在读取数据的时候读取的是<key,value>类型的数据
    * */
    //ssc, kafkaParams, topicMap, StorageLevelv
    /*
    * ssc 值SparkStream
    * kafkaParams  指spark的配置信息
    * topicMap   指sparkStreaming消费的Kafka的topic信息
    * StorageLevel 是指sparkStreaming将数据从Kafka提取出来之后，要优先把提取出来的数据存储到Spark集群中的Worker的内存当中
    * */
    val dstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = dstream.map(_._2) //因为Kafka存储数据是按行存取的.所以_._2 拿出来的是Kafka当中的每一行,以后我们处理数据的时候都是按照行来进行数据处理的
    //lines中存储的是一个RDD，即Kafka中的存储的游戏的Log日志



    //===================================4
    /*
    * 将lines中的每一行拿出来进行切割
    * */
    val splitedLines = lines.map(_.split("\t"))   //将lines中的每一行拿出来按照"\t"来进行切割
    val filteredLines = splitedLines.filter(f => {  //对数据进行过滤
      val et = f(3)  //把事件类型拿出来
      val item = f(8)  //把用户服用了哪些药品拿出来
      et == "11" && item == "强效太阳水"   //如果f的事件类型是11 ，item值是“强效太阳水”，那么这一条数据f就会被拿出来，放到新的RDD当中
    })



    //====================================6
    /*
    *
    * */
    //filteredLines.map... 是把用户名和用户使用这个物品的时间戳拿出来
    //因为这个map算子在运行的时候是被多线程执行的，如果不注意线程问题就会出现错误。因此才需要使用dateFormat这个类，因为这个类是线程安全的
    //groupByKeyAndWindow 即按照group来进行分组，又按照Window来进行分组

    //Milliseconds(30000) 窗口的长度 每隔10000ms产生一个批次RDD。那一个窗口中将能存下3个批次RDD。
    // Milliseconds(20000) 是滑动的长度。每隔20000ms 滑动窗口就会向前移动20000/10000 = 2个批次RDD
    val grouedWindow: DStream[(String, Iterable[Long])] = filteredLines.map(f => (f(7), dateFormat.parse(f(12)).getTime)) //
      .groupByKeyAndWindow(Milliseconds(30000), Milliseconds(20000))




    //======================================7
    /*
    * You know there still may be some mistakes which are produced by the NetSpeed.
    * but I konw,a user of my game could only use 4 bottles drug within the time before he can
     * drink another drugs
     * so,if a guy have used over 4 bottles drugs.It means he is cheating
    * */
    val filtered: DStream[(String, Iterable[Long])] = grouedWindow.filter(_._2.size >= 5)



    //======================================8
    /*
    *filtered is the return of grouedWindow function,which means "val filtered" is a map of RDDs.
    * so the "val it" is a Iterable[Long]
    * */
    val itemAvgTime = filtered.mapValues(it => {
      val list = it.toList.sorted  //using scala's function "sorted" to sort the member of Iterable
      val size = list.size //知道它在这一段时间内使用了多少道具
      val first = list(0)  //get the time of the user drinking the first drug
      val last = list(size - 1) //get the time of the user drinking the last drug

      //ji suan user mei ge duo chang shi jian he yi ci drug
      val cha: Double = last - first
      cha / size
    })



    //====================================9
    //传16-34-07_2
    /*
    * 将use drug jian ge xiao yu 10000 de ti qu chu lai _.2小于10000的数取出来
    * */
    val badUser: DStream[(String, Double)] = itemAvgTime.filter(_._2 < 10000)



    //===================================10
    /*
    *store the result to DateBase
    * */

    badUser.foreachRDD(rdd => {
      rdd.foreachPartition(it => {

        //jiang shu ju xie dao Jedis shu ju ku
        val connection = JedisConnectionPool.getConnection()
        it.foreach(t => {
          val user = t._1
          val avgTime = t._2
          val currentTime = System.currentTimeMillis()
          connection.set(user + "_" + currentTime, avgTime.toString) //
        })
        connection.close()
      })
    })



    filteredLines.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
