package minMax

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MinMax {
  def main(args: Array[String]): Unit = {
    getMinMax()
  } //def main

  def getMinMax() {
    var min = Integer.MAX_VALUE
    var max = Integer.MIN_VALUE
    val inputFile = "hdfs://Master:9000/input/MinMax"
    val regex = """(-)?([0-9]+)"""
    val AppName = "MinMax"
    val conf = new SparkConf().setAppName(AppName)
    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile(inputFile)
    val filterRDD = inputRDD.filter(line =>
      {
        line.trim().matches(regex)
      })
    val mapRDD = filterRDD.map(_.trim().toInt).map(x =>
      {
        if (x < min)
          min = x
        if (x > max)
          max = x
        (min, max)
      })
    mapRDD.collect().map(x =>
      {

        if (x._1 >= x._2) {
          if (x._1 > max)
            max = x._1
          if (x._2 < min)
            min = x._2
        }
        if (x._1 < x._2) {
          if (x._2 > max)
            max = x._2
          if (x._1 < min)
            min = x._1
        }
        (min, max)
      })
    println("max=" + max + " " + "min=" + min)

  } //def getMinMax

}//object MinMax