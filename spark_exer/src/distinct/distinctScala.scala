package distinct

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object distinctScala {
  def main(args:Array[String]): Unit = {
    distinctScore()

  } //def main

  def distinctScore() {
    val delimiter = "\\s+"
    val inputFile = "hdfs://Master:9000/input/distinct"
    val AppName = "distinctScala"
    val conf = new SparkConf().setAppName(AppName)
    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile(inputFile)
    val filterRDD = inputRDD.filter(line =>
      {
        (line.trim().length() > 0) && (line.split(delimiter).length == 2)
      })
    val mapRDD = filterRDD.map(line =>
      {
        val name = line.split(delimiter)(0).trim()
        val score = line.split(delimiter)(1).trim()
        (name + " " + score, "")
      }).distinct().sortBy(_._1,false) //.sortBy(_._1, false).distinct()
    val result = mapRDD.collect().foreach(x => println(x._1))
   
    sc.stop()

  } //def distinctScore

}//object distinctScala