package cn.itcast.spark.day3

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 这个demo是从关系型数据库当中的数据读取到Spark的RDD当中
  */


object JdbcRDDDemo {

  def main(args: Array[String]) {

    //--------------1
    /*
    * 建立spark连接的配置文件
    * */
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)


    //-------------2
    /*
    * 链接数据库的程序
    * 这是将一个方法转换成了函数，函数名字为connection，这个函数没有任何的形参，所以形参的位置写成了 ()'
    * 这个函数的返回值是　connection 类型的数据
    * */
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456")
    }



    //----------------3
    /*
    * 这一步是非常关键的．
    * 使用JdbaRDD创建一个RDD
    * */

    /*
    * /**
 * An RDD that executes an SQL query on a JDBC connection and reads results.
 * For usage example, see test case JdbcRDDSuite.
 *
 * @param getConnection a function that returns an open Connection.
 *   The RDD takes care of closing the connection.
 * @param sql the text of the query.
 *   The query must contain two ? placeholders for parameters used to partition the results.
 *   E.g. "select title, author from books where ? <= id and id <= ?"
 * @param lowerBound the minimum value of the first placeholder
 * @param upperBound the maximum value of the second placeholder
 *   The lower and upper bounds are inclusive.
 * @param numPartitions the number of partitions.
 *   Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
 *   the query would be executed twice, once with (1, 10) and once with (11, 20)
 * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
 *   This should only call getInt, getString, etc; the RDD takes care of calling next.
 *   The default maps a ResultSet to an array of Object.
 */
class JdbcRDD[T: ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging
    * */

    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM ta where id >= ? AND id <= ?", //SQL语句
      1, 4, 2,   //1,4,2  中的２代表这个jdbcRDD的分区数
      r => {     //这也是一个匿名函数
        val id = r.getInt(1)
        val code = r.getString(2)
        (id, code)
      }
    )


    val jrdd = jdbcRDD.collect()
    println(jdbcRDD.collect().toBuffer)
    sc.stop()
  }
}