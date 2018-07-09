package averageScore

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapred.TextInputFormat

object computeAverageScore {
  def main(args: Array[String]): Unit = {
    getAverageScore_2()
    //    getAverageScore()
  } //def main

  def getAverageScore_2() {
    val encode = "GBK"
    val num = 3.toDouble
    val delimiter = "\\s+"
    val inputFile = "hdfs://Master:9000/input/averageScore"
    val appName = "computeAverageScore"

    var pps = System.getProperties();
    pps.setProperty("file.encoding", "UTF-8");

    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    //val input = sc.textFile(inputFile)

    val input = sc.hadoopFile(inputFile, classOf[TextInputFormat],
      classOf[LongWritable], classOf[Text]).map(
      pair => new String(pair._2.getBytes, 0, pair._2.getLength, encode))

    val mapResult = input.map(line =>
      {
        (line.split(delimiter)(0).trim(), line.split(delimiter)(1).trim().toInt)
      })
    val result = mapResult.reduceByKey((x, y) => x + y).map(x =>
      {
        (x._1, x._2.toDouble / num.toDouble)
      }).collect()
    val title = "姓名     平均成绩".getBytes("UTF-8")
    println(title)
    result.foreach(x => println(x._1 + "  " + x._2))
    sc.stop()
  }

  def getAverageScore() {
    val num = 3
    val delimiter = "\\s+"
    val inputFile = "hdfs://Master:9000/input/averageScore"
    val appName = "computeAverageScore"

    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val mapResult = input.map(line =>
      {
        (line.split(delimiter)(0).trim(), line.split(delimiter)(1).trim().toInt)
      })
    val result = mapResult.reduceByKey((x, y) => x + y).map(x =>
      {
        (x._1, x._2 / num)
      }).collect()
    println("姓名     平均成绩")
    result.foreach(x => println(x._1 + "  " + x._2))
    sc.stop()
  } //def getAverageScore
}