package countIpAddress

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CountIpNumberOnVideo {
  /*  统计每个视频独立IP数,有时我们不但需要知道全网访问的独立IP数，更想知道每个视频访问的独立IP数*/
  //  223.93.159.226 HIT 203 [15/Feb/2017:11:14:35 +0800] "GET http://v-cdn.abc.com.cn/141035.mp4 HTTP/1.1" 206 5444007

  def main(args: Array[String]): Unit = {
    CountIpNumberOnVideo_1()
  }
  val videoPattern = "([0-9]+)\\.mp4".r
  val ipPattern = "((25[0-5]|2[0-4]\\d|[01]?\\d\\d)\\.){3}(25[0-5]|2[0-4]\\d|[01]?\\d\\d)".r

  def getVideoNameAndIpAdress(line: String) = {
    (videoPattern.findFirstIn(line).mkString, ipPattern.findFirstIn(line).mkString)
  }

  def CountIpNumberOnVideo_1() {
    val conf = new SparkConf().setAppName("CountIpNumberOnVideo")
    val sc = new SparkContext(conf)

    val inputFile = "hdfs://Master:9000/input/cdn.csv"
    val input = sc.textFile(inputFile)
    input.filter(x => x.matches(".*([0-9]+)\\.mp4.*")).
    map(line => (videoPattern.findFirstIn(line).mkString, ipPattern.findFirstIn(line).mkString)).
      groupByKey().map(x => (x._1, x._2.toList.distinct)).sortBy(_._2.size, false).
      take(100000).foreach(x => println("video: " + x._1 + " IpNum: " + x._2.size))
  }
}