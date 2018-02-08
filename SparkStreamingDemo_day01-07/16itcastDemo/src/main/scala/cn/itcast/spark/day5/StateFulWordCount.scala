package cn.itcast.spark.day5

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by root on 2016/5/21.
  */
object StateFulWordCount {



  //----------------３
  //Seq,即iter，代表这个批次某个单词,即key,的次数
  //Option[Int]：以前的结果．即这个单词在以前的RDD中的累加和

  /*
  * 笔记２的map((_,1))这个过程，是把SparkStreaming收集到的所有内容，按照＂　＂，空格来进行切割，
  * 如＂hello tom jerrry＂
  * 变成(hello,1),(tom,1),(jerry,1)
  * 这个Seq中为什么是一个［string,Seq[Int]］类型的呢?
  * 这个Seq存储的是当前的批次的key值相同的value组成的集合
  * 如，当前批次中的单词是
  * hello tom
  * hello jerry
  * hello mark
  * 那么
  * 把这些单词切分开之后
  * (hello,1) (tom,1)
  * (hello,1) (jerry,1)
  * (hello,1) (mark,1)
  * Seq这个形参中存放的就是
  * (hello,(1,1,1))
  * (tom(1))
  * (jerry,(1))
  * (mark,(1))
  * */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {


    //it._2.sum 代表Seq[Int]的加和；it._3代表OPtion[Int]，即前面的RDD的累加和
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))

    //iter.map{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}

    //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))  //这个一行写的也正确

    //case是模式匹配的类．当iter中的每一个元素都是(String, Seq[Int], Option[Int])类型的，则(word, current_count, history_count)
    //中，word对应String，current_count对应Seq[Int]，history_count对应Option[Int]
    //current_count.sum是先让Seq[Int]进行累加和，再和之前的RDD进行加和．如(hello,(1,1,1),5)
    //加完之后成为(hello,1+1+1+5),即(hello,8)
    iter.map{ case(word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0)) }
  }

  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels()
    //StreamingContext


    //--------------1
    /*
    * 设定一个SparrkStreaming的配置项
    * */
    val conf = new SparkConf().setAppName("StateFulWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //-------------4
    //updateStateByKey必须设置setCheckpointDir，即设置一个Checkpoint的工作目录
    /*
    * 如果程序运行到一半出现了问题了，就会把上一次程序运行出现问题的地方记下来．
    * 下一次接着从上一次运行停止的位置开始运行
    * */
    sc.setCheckpointDir("c://ck")


    val ssc = new StreamingContext(sc, Seconds(5))

    val ds = ssc.socketTextStream("172.16.0.11", 8888)


    //---------------------3
    /*
    * 调用updateStateByKey这个算子，将SparkStreaming在收集数据的时间段内，所有key相同的键值对[key,value]中的value进行累加
    * param:
    * updateFunc 指把各个RDD，key相等的value的处理方法．在这个函数中，你可以让这些value相加，相减，总之在这个函数中实现你的逻辑
    * HashPartitioner　这个参数是key值分区的方法．即我的key通过这个方法来确定，在处理完成后，将这个key放在哪一个分区当中
    * 形参true　的含义是是否记住之前批次RDD的分区的方法
    * */
    val result = ds.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)

    result.print()

    ssc.start()

    ssc.awaitTermination()

  }
}