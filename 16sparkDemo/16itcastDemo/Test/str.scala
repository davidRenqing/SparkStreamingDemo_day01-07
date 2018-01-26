/**
  * Created by hadoop01 on 18-1-23.
  */
object str {
  def main(args: Array[String]): Unit = {
    var ip = "192.168.1.1"
    val fragments = ip.split("[.]")  //先把ip地址按照 "." 进行切分
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      var num1 = fragments(i).toLong
      var num2 = ipNum << 8L

      ipNum =  num1 | num2 //左移8位，就变成了10进制
    }
    ipNum
  }
}
