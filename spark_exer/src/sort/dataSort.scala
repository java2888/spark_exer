package sort

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

object dataSort {

  def main(args: Array[String]): Unit = {
    sort()
  } //def main

  def sort() {
    var i = 0
    //scala之正则表达式（一）基础匹配 https://blog.csdn.net/legotime/article/details/51607159
    // input.filter(x => x.matches(".*([0-9]+)\\.mp4.*")).
    val regex = """(-)?([0-9]+)"""
    val inputFile = "hdfs://Master:9000/input/sort"
    val AppName = "dataSort"
    val conf = new SparkConf().setAppName(AppName)
    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile(inputFile)
    val filterRDD = inputRDD.filter(line =>
      {
        line.trim().matches(regex)
      })
    val mapRDD = filterRDD.map(line => (line.trim().toInt,"")).partitionBy(new HashPartitioner(1))
    val sortRDD = mapRDD.keys.sortBy(x => x, true).collect()
    val result = sortRDD.foreach(x =>
      {
        i = i + 1
        println(i + " " + x)

      })

  } //def sort

}//object dataSort