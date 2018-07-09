package someSparkExample

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import scala.util.Random
import org.apache.spark.{ SparkConf, SparkContext }

object tallestAndshortestHeight {

  def main(args: Array[String]): Unit = {
    getHeightInfo()
  } //def main

  def getHeightInfo() {
    val inputPath = "hdfs://Master:9000/input/height/height.txt"
    createHdfsInputFile(inputPath)

    val conf = new SparkConf().setAppName("getHeightInfo")
    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile(inputPath)
    val filterRDD = inputRDD.filter(line =>
      {
        line.split("\t")(0).trim().matches("""[0-9]+""") &&
          line.split("\t")(1).matches("""[F|M]""") &&
          line.split("\t")(2).matches("""[0-9]{2,3}""")
      })
    val manRDD = filterRDD.filter(line =>
      {
        line.split("\t")(1).trim().matches("""M""")
      }).map(line => line.split("\t")(2).toLong)
    val womanRDD = filterRDD.filter(line =>
      {
        line.split("\t")(1).matches("""F""")
      }).map(line => line.split("\t")(2).toLong)
    val manCount = manRDD.count()
    val womanCount = womanRDD.count()
    val manTallest = manRDD.sortBy(x => x, false).first()
    val manShortest = manRDD.sortBy(x => x, true).first()
    val womanTallest = womanRDD.sortBy(x => x, false).first()
    val womanShortest = womanRDD.sortBy(x => x, true).first()
    sc.stop()

    val result = "manCount=" + manCount + "; womanCount=" + womanCount + "\n"+
    " manTallest=" + manTallest + "; manShortest=" + manShortest +  "\n"+
    " womanTallest=" + womanTallest + "; womanShortest=" + womanShortest
    val outputFile = "hdfs://Master:9000/output/height/heightResult.txt"
    outputHdfsFile(result, outputFile)
  }

  def getGender(): String = {
    val rand = new Random()
    val gender = rand.nextInt(2)
    if (gender == 1)
      return "M"
    else
      return "F"
  }

  def getHeight(): String = {
    val rand = new Random()
    var height = rand.nextInt(200)
    if (height < 100) {
      height = height + 100
    }
    return height.toString()
  }

  def createHdfsInputFile(path: String) {
    val Num: Long = 1000000L
    val inputFile = new Path(path)
    val hdfs = FileSystem.get(new Configuration())
    if (hdfs.exists(inputFile)) {
      hdfs.delete(inputFile,true)
    }

    val outputStream: FSDataOutputStream = hdfs.create(inputFile, true)
    var rand = new Random()
    for (i <- 1L to Num) {
      val gender = getGender()
      val height = getHeight()
      val content = i + "\t" + gender + "\t" + height
      outputStream.writeBytes(content)
      outputStream.writeBytes(System.getProperty("line.separator"))
    }
    outputStream.flush()
    outputStream.close()

  } //def createHdfsInputFile

  def outputHdfsFile(content: String, path: String) {
    val hdfs = FileSystem.get(new Configuration())
    val file = new Path(path)
    if (hdfs.exists(file)) {
      hdfs.delete(file,true)
    }
    val outputStream: FSDataOutputStream = hdfs.create(file, true)
    outputStream.writeBytes(content)

  } //def outputHdfsFile

}//object height