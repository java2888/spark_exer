package logAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//logAnalysis.logInfoAnalysis
object logInfoAnalysis {
   def main(args:Array[String]):Unit={
     dealWithLog()
   }
   
//196.168.2.1 - - [03/Jul/2014:23:36:38 +0800] "GET /course/detail/3.htm HTTP/1.0" 200 38435 0.038  
//182.131.89.195 - - [03/Jul/2014:23:37:43 +0800] "GET /html/notes/20140617/888.html HTTP/1.0" 301 - 0.000  
   
// input.filter(x => x.matches(".*([0-9]+)\\.mp4.*")).
   
   def dealWithLog(){
     val Mattern=".*\\[.*\\]\\s\"(GET|POST).*\\sHTTP\\/1\\.0\".*"
     val inputFile="hdfs://Master:9000/input/logAnalysis/logAnalysis.txt"
     val conf=new SparkConf().setAppName("LogAnalysis")
     val sc=new SparkContext(conf)
     val input=sc.textFile(inputFile)
     val filterResult=input.filter(line=>{line.matches(Mattern)})
     val result=filterResult.map(line=>
       {
         val methodAndUrl=line.split("\"")(1).trim().split("\\s+")(0).trim()+" "+
                         line.split("\"")(1).trim().split("\\s+")(1).trim()
         (methodAndUrl,1)
       }
     ).reduceByKey((x,y)=>x+y)
     println("result.count="+result.count())
     result.collect().foreach(x=>println("("+x._1+" "+","+x._2+")"))
     sc.stop()
   }
}