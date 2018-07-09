package countIpAddress

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CountIpAddress {
  def main(args: Array[String]): Unit = {
    countIp_2()
  } //main

def countIp_2():Unit={
  val inputFile="hdfs://Master:9000/input/cdn.csv"
  val appName="CountIpAddress"
  val IpPattern="((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)".r
  val conf=new SparkConf().setAppName(appName)
  val sc=new SparkContext(conf)
  val input=sc.textFile(inputFile).flatMap(x=>IpPattern.findAllIn(x)).map(x=>(x,1)).reduceByKey((x,y)=> x+y ).sortBy(_._2, false)
  val x=input.count()
  println("ip数为:"+x)
  input.take(100000000).foreach(println)
 }

}//CountIpAddress