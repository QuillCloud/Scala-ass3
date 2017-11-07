package comp9313.ass3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem2 {
  def main(args: Array[String]) {
    // take 2 parameters
    val inputFile = args(0)
    val outputFolder = args(1)
    // create a Spark context object with the spark configuration
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)
    // read file
    val input =  sc.textFile(inputFile)
    // split each line and make each line as a pair(second substring is key, first substring is value)
    val nodes = input.map(line=>(line.split("\\s+")(1), line.split("\\s+")(0)))
    // sort the node, then reduce by key, then sort again, finally transform to the format that desired
    val merge_nodes = nodes.sortBy(f=>f._2.toInt, true).reduceByKey(_+","+_).sortBy(f=>f._1.toInt,true).map(f=>f._1 + "\t" + f._2)
    // write result to folder
    merge_nodes.saveAsTextFile(outputFolder)
   }
}  