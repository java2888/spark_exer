package countIpAddress

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//Scala中正则表达式以及与模式匹配结合
//https://blog.csdn.net/yizheyouye/article/details/49204595

object Test {
  def main(args: Array[String]): Unit = {

    val regex = """([0-9]+)([a-z]+)""".r //"""原生表达
    val numPattern = "[0-9]+".r
    val numberPattern = """\s+[0-9]+\s+""".r

    //findAllIn()方法返回遍历所有匹配项的迭代器
    for (matchString <- numPattern.findAllIn("99345 Scala,22298 Spark"))
      println(matchString)

    //找到首个匹配项
    println(numberPattern.findFirstIn("99ss java, 3222 spark, 333 hadoop"))

    //数字和字母的组合正则表达式
    val numitemPattern = """([0-9]+) ([a-z]+)""".r

    val numitemPattern(num, item) = "99 hadoop"

    val line = "93459 hspark"
    line match {
      case numitemPattern(num, blog) => println(num + "\t" + blog)
      case _ => println("hahaha...")
    }

    println("x+y=" + myFunc(3, 5))

  }

  def myFunc(x: Int, y: Int): Int = {
    x + y
  }

  def countIp(): Unit = {
    val conf = new SparkConf().setAppName("CountIpAddress").setMaster("yarn-cluster")
    val sc = new SparkContext(conf)
    val inputFile = "hdfs://Master:9000/input/cdn.csv"

    val input = sc.textFile(inputFile)
    //匹配IP地址正则
    val IPPattern = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

    //1.统计独立IP数
    val ipNums = input.flatMap(x => IPPattern.findFirstIn(x)).map(x => (x, 1)).reduceByKey((x, y) => x + y).sortBy(_._2, false)
    //输出IP访问数前量前10位
    ipNums.take(10).foreach(println)
    println("独立IP数：" + ipNums.count())

  }

  def countIp_1(): Unit = {
    val inputFile = "hdfs://Master:9000/input/cdn.csv"
    val conf = new SparkConf().setAppName("CountIpAddress")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val IpPattern = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)".r
    val IpNumber = input.flatMap(x => IpPattern.findAllIn(x)).map(word => (word, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
    val x = IpNumber.count()
    println("--------------------------")
    println("Ip总数:" + x)
    println("--------------------------")
    IpNumber.take(100000000).foreach(println)
  } //countIp_1

  /*  //匹配文件名
  val  fileNamePattern="([0-9]+).mp4".r
  def getFileNameAndIp(line:String)={
    (fileNamePattern.findFirstIn(line).mkString,IPPattern.findFirstIn(line).mkString)
  }
  //2.统计每个视频独立IP数
    input.filter(x=>x.matches(".*([0-9]+)\\.mp4.*")).map(x=>getFileNameAndIp(x)).groupByKey().map(x=>(x._1,x._2.toList.distinct)).
      sortBy(_._2.size,false).take(10).foreach(x=>println("视频："+x._1+" 独立IP数:"+x._2.size))
*/
}