package cn.congshuo.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop01 on 18-1-22.
  */
object str {

  def main(args: Array[String]): Unit = {

    //只要把这两个对象创建出来之后,就可以构建sc在这个对象.之后调用这些函数,就可以进行进入函数的内部看到这个函数的源码了
    val conf = new SparkConf().setAppName("customSort").setMaster("local")
    //    val conf=ne w SparkConf()
    val sc = new SparkContext(conf)

    val a = sc.parallelize(1 to 9, 3) //对1...9,分成3个分区

    def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = { //T 代表RDD当中的元素的类型,Int或者Array 都是可以的

      println("111111111111111")
      var res = List[(T, T)]() //定义一个List，这个List的名字是　res

      var pre = iter.next

      while (iter.hasNext) {
        val cur = iter.next
        res.::=(pre, cur) //向res List数组中添加元素
        pre = cur
      }

      res.iterator //返回res 这个List对象
    }

    //一开始我不管怎么调试,都进不去我自己定义的函数的内部.
    //原来spark的RDD是延迟加载的,在val str2=a.mapPartitions(myfunc)
    // 这一行代码当中,是延迟加载的,所以执行到collect阶段之后就会进入你自己定义的函数数当中了

    //针对这种情况,如果想要调试你自己定义的函数,就在你要调试的函数当中添加一个断点
    // 然后使用跳转到下一个断点的方法,就可以转到你要调试的函数当中了
    val str2 = a.mapPartitions(myfunc)
    print(str2.collect())
  }
}