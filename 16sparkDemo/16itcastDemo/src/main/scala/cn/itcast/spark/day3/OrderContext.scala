package cn.itcast.spark.day3

import org.apache.spark.{SparkConf, SparkContext}



//--------------3
/*
*现在我要搞一个隐式转换函数和方法
* */
object OrderContext {


  /*
  * 这段代码的逻辑就是，把我自己写的Girl类,隐式转换成Orderding类．
  * 之后重写了Orderding类的compare函数
  * */
  //
  implicit val girlOrdering  = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue > y.faceValue) 1
      else if (x.faceValue == y.faceValue) {
        if(x.age > y.age) -1 else 1
      } else -1
    }
  }
}




/**
  * Created by root on 2016/5/18.
  */
//sort =>规则 先按faveValue，比较年龄
//name,faveValue,age


object CustomSort {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc = new SparkContext(conf)



    //-----------1
    /*
    * 模拟一些与人相关的数据，数据格式是(name,faveValue,age,编号)
    * */
    val rdd1 = sc.parallelize(List(("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2),("JuJingYi", 95, 22, 3)))

    //----------4
    /*
    * 这里代表使用隐式转换，柯里化的方法
    * */
    import OrderContext._


    //-------------５
    /*
    * 让rdd1在调用sortBy算子时使用Girl类的比较规则来排序
    * sortBy中的false代表从小到大的排序方法
    * */
    val rdd2 = rdd1.sortBy(x => Girl(x._2, x._3), false)
    println(rdd2.collect().toBuffer)
    sc.stop()
  }

}




//----------------2
/*
隐式转换中
* 这个Girl 类的作用就是封装数据．
* 将我用于排序的两个值　faceValue和age　作为Girl 类的形参
* */
/**
  * 第二种，通过隐式转换完成排序
  * @param faceValue
  * @param age
  */
case class Girl(faceValue: Int, age: Int) extends Serializable