package someSparkExample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.FileWriter
import java.io.File
import org.apache.hadoop.fs._
import scala.util.Random
import org.apache.hadoop._

object averageAge {
  def main(args: Array[String]): Unit = {
    val inputFile = "hdfs://Master:9000/input/averageAge.txt"
    createHDFSFile(inputFile)
    getAverageAge(inputFile)
  } //def main

  def getAverageAge(inputFile: String) {
    val delimiter = "\\s+"
    val numRegex = """[0-9]{1,}"""
    val ageRegex = """[0-9]{1,3}"""
    //    val inputFile = "hdfs://Master:9000/input/averageAge.txt"
    val outputFile = "hdfs://Master:9000/output/averageAgeResult.txt"
    val AppName = "average"
    val conf = new SparkConf().setAppName(AppName)
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val filterRDD = input.filter(line =>
      {
        val num = line.split(delimiter)(0).trim()
        val age = line.split(delimiter)(1).trim()
        num.matches(numRegex) && age.matches(ageRegex)
      })
    val mapRDD = filterRDD.map(line =>
      {
        val age = line.split(delimiter)(1).trim()
        (age)
      })
    val count = mapRDD.count()
    val result = mapRDD.reduce((x, y) =>
      {
        val a: Long = x.toLong
        val b: Long = y.toLong
        (a + b).toString()
      })
    println("count=" + count + " result=" + result)
    println("average:" + result.toDouble / count.toLong)

    val content = "count=" + count + " result=" + result + System.getProperty("line.separator") +
      "average:" + result.toDouble / count.toLong
    outputHDFSFile(outputFile, content)
  } //def getAverageAge

  def outputHDFSFile(path: String, content: String) {
    //val path = "hdfs://Master:9000/input/b.txt"
    val output = new Path(path)
    val hdfs = FileSystem.get(new conf.Configuration())
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
      println("delete!--" + path)
    }
    hdfs.create(output, true)
    println("create!--" + path)
    println(hdfs.getFileStatus(output))
    println(hdfs.getHomeDirectory)
    val outputStream: FSDataOutputStream = hdfs.create(output)
    outputStream.writeBytes(content)
    outputStream.flush()
    outputStream.close()
  } //def createHDFSFile

  def createHDFSFile(path: String) {
    //val path = "hdfs://Master:9000/input/b.txt"
    val output = new Path(path)
    val hdfs = FileSystem.get(new conf.Configuration())
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
      println("delete!--" + path)
    }
 
    hdfs.create(output, true)
    println("create!--" + path)
    println(hdfs.getFileStatus(output))
    println(hdfs.getHomeDirectory)
    val outputStream: FSDataOutputStream = hdfs.create(output)
    var rand = new Random()
    for (i <- 1 to 1000000) {
      var age = rand.nextInt(100)
      outputStream.writeBytes(i.toString() + "\t" + age)
      outputStream.writeBytes(System.getProperty("line.separator"))
    }
    outputStream.flush()
    outputStream.close()
  } //def createHDFSFile

  /*  def getLocalInputFile(inputFile: String) {
    val a = new FileWriter(new File(inputFile), false)
    var rand = new Random()
    for (i <- 1 to 1000000) {
      var age = rand.nextInt(100)
      a.write(i.toString() + "\t" + age)
      a.write(System.getProperty("line.separator"))
    }
    a.flush()
    a.close()
  } //def getLocalInputFile
*/
}//object averageAge