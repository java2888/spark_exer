package top_K

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.control.Exception.Finally

object topK {
   def main(args:Array[String]):Unit={
    topK_func_3()
  }

  
  def topK_func_3(){
    var count=0
    val inputFile = "hdfs://Master:9000/input/topK"
    val delimiter = ","
    val conf = new SparkConf().setAppName("topK")
    val sc = new SparkContext(conf)
    val six = sc.textFile(inputFile)  
//    val res = six.map(x=>x).collect().foreach(x => {println(x)}) 
    val res = six.filter(x => (x.trim().length>0) && (x.split(",").length==4))
.map(_.split(",")(2)).map(x => (x.toInt,"")).sortByKey(false).map(x=>x._1)
.take(500)  
.foreach(x =>
  {
    count=count+1
    println("count="+count+"  payment="+x)
    
  }
  ) 
}  
  
  def topK_func_2(){
    val inputFile = "hdfs://Master:9000/input/topK/a.txt"
    val delimiter = ","
    val conf = new SparkConf().setAppName("topK")
    val sc = new SparkContext(conf)
    val six = sc.textFile(inputFile)  
var idx = 0;  
val res = six.filter(x => (x.trim().length>0) && (x.split(",").length==4))
.map(_.split(",")(2)).map(x => (x.toInt,"")).sortByKey(false).map(x=>x._1)
.take(5)  
.foreach(x => {  
idx = idx+1  
println(idx +"\t"+x)}) 
  }
  
  def topK_func() {
    var i=0
    var count = 0 
    val n = 5000
    val inputFile = "hdfs://Master:9000/input/topK"
    val delimiter = ","
    val conf = new SparkConf().setAppName("topK")
    val sc = new SparkContext(conf)
    try{
    i=1
    val input = sc.textFile(inputFile).filter(line =>
        {
          (line.trim().length() > 0) && (line.trim().split(",").length == 4)
        }).map(line => line.split(",")(2).toInt) 
     i=i+1
    val map1=input.sortBy(x => x, false) 
    i=i+1
     val map2=map1.collect().map(x=>
       {
         count=count+1
         (count,x)
        } 
     ) 
     
     i=i+1
     val collectResult=map2.take(n).
      foreach(x => println("num=" + x._1 + " payment=" + x._2))
     i=i+1 
    }catch{
      case ex: Throwable =>println("found a unknown exception"+ ex)  
    }finally{
      println("Exiting finally")
      println("i="+i)
    }
  }

}