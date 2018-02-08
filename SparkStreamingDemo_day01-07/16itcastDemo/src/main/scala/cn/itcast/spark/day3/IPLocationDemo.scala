package cn.itcast.spark.day3

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import scala.collection.mutable.ArrayBuffer

object IPLocationDemo {

  //-------------1
  //将ip从"192.168.1.1"转换为十进制的数
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")  //先把ip地址按照 "." 进行切分
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L //左移8位，就变成了10进制
    }
    ipNum
  }

  def readData(path: String) = {
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
    var s: String = null
    var flag = true
    val lines = new ArrayBuffer[String]()
    while (flag)
    {
      s = br.readLine()
      if (s != null)
        lines += s
      else
        flag = false
    }
    lines
  }



  //----------3
  /*
  * 每个地区的ＩＰ是十进制段．比如
  * 1.2.0.0|1.2.1.255|16908288|16908799|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
  * 亚洲|中国|福建|福州||电信 的ＩＰ区间就是　16908288~16908799
  *
  * */


  def binarySearch(lines: ArrayBuffer[String], ip: Long) : Int = {

    //因为老师给的IP.txt哪个文件ＩＰ已经从大到小排好顺序了．这样我就可以使用二分法来计算我的ＩＰ属于哪个区间了
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle).split("\\|")(2).toLong) && (ip <= lines(middle).split("\\|")(3).toLong))
        return middle
      if (ip < lines(middle).split("\\|")(2).toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }



  def main(args: Array[String]) {
    val ip = "120.55.185.61"
    val ipNum = ip2Long(ip)
    println(ipNum)


    //-----------2
    /*
    * 我的ip文件中存储着，ip与地区的对应
    * 我可以将这些IP信息存储在数据库中．将这些ip和地区　的对应信息，放在数据库中
    * 但是将这些对应的IP和地区的对应关系放在内存中去．
    * 然后最好是，这些转换成１０进制的数据最好是事先排好序的，这样我就可以使用二分法来查找IP信息了
    * 你看binarySearch　这个函数就可以了
    * */
    val lines = readData("/home/hadoop01/sparkTest/ip.txt")
    val index = binarySearch(lines, ipNum)
    print(lines(index))
  }
}