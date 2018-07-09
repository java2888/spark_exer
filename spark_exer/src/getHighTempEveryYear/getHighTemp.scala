package getHighTempEveryYear

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object getHighTemp {
   def main(args:Array[String]):Unit={
     getResult()
   }
   
   def getResult(){
     val inputFile="hdfs://Master:9000/input/getHighTempEveryYear.txt"
     val conf=new SparkConf().setAppName("getHighTempEveryYear")
     val sc=new SparkContext(conf)
     val input=sc.textFile(inputFile)
     val all=input.filter(line=>
          {
            val temp=line.substring(46, 50).toInt
            val value_50th=line.charAt(50).toString()
            (temp.!=(9999)) &&  (value_50th.matches( "[01459]" ))                 
          }        
        ).map(line=>
          {
            val year=line.substring(15,19).toInt
            val temp={
              if(line.charAt(45).==('+')){
                line.substring(46,50).toInt
              }else{
                line.substring(45,50).toInt
              }
            }
            (year,temp)
          }
        ) 
      Print()
      all.collect().foreach(x=>println("year:"+x._1+" temp:"+x._2))
      Print()
      val result=all.reduceByKey( 
            (x, y)=>{if(x>y)  x else y}
        ).collect().foreach(x=>println("year:"+x._1+" temp="+x._2) )
       println("end")
   }
   
   def Print(){
     println("-------------------------")
   }
}