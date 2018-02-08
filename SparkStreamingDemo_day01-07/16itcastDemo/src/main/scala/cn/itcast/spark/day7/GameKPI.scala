package cn.itcast.spark.day7

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/24.
  */
object GameKPI {

  def main(args: Array[String]) {


    //-----------------------------4
    /*
    * 设定一个时间，以后让系统根据今天是星期几自动计算当前的beginTime和endTime
    * */
    val queryTime = "2016-02-02 00:00:00"
    val beginTime = TimeUtils(queryTime)

    //------------------------------10
    /*
    * TimeUtils是一个对时间进行操作的类
    * 具体看TImeUtils.scala这个类
    * */
    val endTime = TimeUtils.getCertainDayTime(+1)


    //---------------------------------1
    //搞一个sparkConf
    //local[2],代表使用local的方式运行程序．每台机器使用２线程．local[*]，*代表根据我的机器的配置的核数，根据机器的核数来自动地启动计算启动多少个线程
    val conf = new SparkConf().setAppName("GameKPI").setMaster("local[*]")
    val sc = new SparkContext(conf)



    //-----------------------------2
    //导入数据．
    /*
    * 以后可以把每天产生的Log数据放在同一个分区当中．第一天地放在分区１，第２天的放在分区２
    *这样以后在导出数据的时候，就直接使用HDFS的目录就可以把数据导出来了
    * */
    //切分之后的数据
    val splitedLogs = sc.textFile("c://GameLog.txt").map(_.split("\\|"))//|线得进行转移才可以
    //过滤后并缓冲

    //----------------------------------3
    /*
    * 以后你会发现，你需要经常使用beginTime和endTime　对数据进行过滤
    * 也就是说　splitedLogs.filter(beginTime).falter(endTime)这个算子操作要经常进行使用．
    * 这时候就可以考虑把这个经常使用的操作封装到类filterByTime　当中了
    * */

    //-----------------------------5
    /*
    * 创建一个单例对象FilterUtils.这个函数中要定义filterByTime方法，将开始时间和结束时间都封装在这个filterByTime方法中
    * 具体要去看FilterUtils.scala 这个文件
    * */

    //---------------------------------12
    /*
    * 对数据进行过滤并且缓存
    * 因为我对数据进行过滤之后的数据是在计算各个指标的时候经常使用的．所以要把数据放到cache当中．这样以后我就可以经常使用我的数据了
    * */
    //你要利用一次数据处理的方式,将数据进行放在cache中之后,要同时计算多个指标
    val filteredLogs = splitedLogs.filter(fields => FilterUtils.filterByTime(fields, beginTime, endTime))
      .cache()




    //----------------------------------13
    /*
    * 计算今天的注册的信号
    * EventType.REGISTER　的值是1.在一个EventType.scala当中
    * */
    //日新增用户数，Daily New Users 缩写 DNU
    val dnu = filteredLogs.filter(fields => FilterUtils.filterByType(fields, EventType.REGISTER))
      .count()


    //-----------------------------------14
    /*
    * 计算日活跃用户的数目.
    * 注册了用户同时登录进去了
    * 日活跃用户就是今天注册和今天登录的用户
    * */
    //日活跃用户数 DAU （Daily Active Users）
    val dau = filteredLogs.filter(fields => FilterUtils.filterByTypes(fields, EventType.REGISTER, EventType.LOGIN))
      .map(_ (3))
      .distinct()
      .count()




    //----------------------------------15

    //  留存率：某段时间的新增用户数的数目记为A，经过一段时间后，仍然使用的用户占新增用户A的比例即为留存率
    //  次日留存率（Day 1 Retention Ratio） Retention Ratio
    //  日新增用户在+1日登陆的用户占新增用户的比例
    val t1 = TimeUtils.getCertainDayTime(-1)



    //---------------------------18
    /*
    * 留存率是在2016-02-02 才开始进行计算的.
    * 因为最早用户登录的时候是2016-02-01 号注册,这样等到2016-02-02才可以用来计算存留率
    * */

    //在一段时间内新注册的用户
    val lastDayRegUser = splitedLogs.filter(fields => FilterUtils.filterByTypeAndTime(fields, EventType.REGISTER, t1, beginTime))
      .map(x => (x(3), 1))

    //计算出在这段时间内新注册的用户有多少又重新登录了
    val todayLoginUser = filteredLogs.filter(fields => FilterUtils.filterByType(fields, EventType.LOGIN))
      .map(x => (x(3), 1))
      .distinct()

    //将第一天注册的用户的RDD和第二天登录的用户的RDD中的key相同的元素提取出来.
    //强制类型转换成Double的数据类型
    val d1r: Double = lastDayRegUser.join(todayLoginUser).count()
    //println(d1r)
    val d1rr = d1r / lastDayRegUser.count()
    println(d1rr)


    //在这里你可以可以把数据写到数据库当中

    sc.stop()
  }
}
// create table GameKPI (id, gamename, zone, datetime, dnu, dau, d1rr, d7rr ... )