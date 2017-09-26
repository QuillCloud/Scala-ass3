package comp9313.ass3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1 {
  def main(args: Array[String]) {
    // take 3 parameters
    val inputFile = args(0)
    val outputFolder = args(1)
    val k = Integer.parseInt(args(2))
    // create a Spark context object with the spark configuration
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    // read file
    val input =  sc.textFile(inputFile)
    // get words for each line, a word only count once in one line
    val words = input.flatMap(line => line.toLowerCase().split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").filter(_.nonEmpty).distinct)
    val w = words.filter(_.charAt(0) >= 'a').filter(_.charAt(0) <= 'z')
    // make pairs and reduce by key
    val counts = w.map(w => (w, 1)).reduceByKey(_+_)
    // sort the pairs by value, transform them to format that desired and get top k pairs
    val output = counts.sortBy(f => f._2, false).map(f => f._1 + "\t" + f._2).take(k)
    // write result to output folder
    sc.parallelize(output).saveAsTextFile(outputFolder)
   }
}