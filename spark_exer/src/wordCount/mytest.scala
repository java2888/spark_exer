package wordCount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs

object mytest {
  def main(args: Array[String]) {
    //    val masterIp="spark://Master:7077"
    //    val conf=new SparkConf().setAppName("WordCount").setMaster(masterIp)
    val conf = new SparkConf().setAppName("mytest").setMaster("yarn-cluster")
    val sc = new SparkContext(conf)
    val file = "hdfs://Master:9000/input/word.txt"

    //    def conf = new SparkConf().setAppName("WordCountSorted").setMaster("local")
    //    def sc = new SparkContext(conf)

    val lines = sc.textFile(file)
    val words = lines.flatMap(_.split(" ")).filter(word => word != " ")
    val pairs = words.map(word => (word, 1))

    val outputPath = "hdfs://Master:9000/output"
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new org.apache.hadoop.fs.Path(outputPath)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }

    /**
     * 在这里通过reduceByKey方法之后可以获得每个单词出现的次数
     * 第一个map将单词和出现的次数交换，将出现的次数作为key，使用sortByKey进行排序（false为降序）
     * 第二个map将出现的次数和单词交换，这样还是恢复到以单词作为key
     */
    //    val wordcount = pairs.reduceByKey(_ + _).map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1))
    //    wordcount.collect.foreach(println)
    val wordcount = pairs.reduceByKey(_ + _).map(pair => (pair._2, pair._1)).
     sortByKey(false).map(pair => (pair._2, pair._1))
    wordcount.collect
    wordcount.saveAsTextFile(outputPath)
    sc.stop()
  }
}