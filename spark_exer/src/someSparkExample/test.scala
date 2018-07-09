package someSparkExample

import java.io.File
import java.io.FileWriter
import scala.util.Random
import org.apache.hadoop.fs.Hdfs

object test {
  def main(args: Array[String]): Unit = {
    val inputFile = "hdfs://Master:9000/input/averageAge.txt"
    createHDFSFile(inputFile)
  }

  def getLocalInputFile(inputFile: String) {
    val a = new FileWriter(new File(inputFile), false)
    var rand = new Random()
    for (i <- 1 to 1000000) {
      var age = rand.nextInt(100)
      a.write(i.toString() + "\t" + age)
      a.write(System.getProperty("line.separator"))
    }
    a.flush()
    a.close()
  }

  def createHDFSFile(path:String) {
    //val path = "hdfs://Master:9000/input/b.txt"
    val output = new org.apache.hadoop.fs.Path(path)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
      println("delete!--" + path)
    }
    hdfs.create(output, true)
    println("create!--" + path)
    println(hdfs.getFileStatus(output))
    println(hdfs.getHomeDirectory)
    val outputStream : org.apache.hadoop.fs.FSDataOutputStream = hdfs.create(output)
    var rand = new scala.util.Random()
    for (i <- 1 to 1000000) {
      var age = rand.nextInt(100)
      outputStream.writeBytes(i.toString() + "\t" + age)
      outputStream.writeBytes(System.getProperty("line.separator"))
    }
    outputStream.flush()
    outputStream.close()
  }//def createHDFSFile
}