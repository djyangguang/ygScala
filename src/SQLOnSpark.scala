import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
 
case class Person(name: String, age: Int)
 
object SQLOnSpark {/*
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQLOnSpark")
    val sc = new SparkContext(conf)
 
    val sqlContext = new SQLContext(sc)
    import sqlContext._
 
    val people: RDD[Person] = sc.textFile("hdfs://hadoop1:9000/class6/people.txt")
      .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    people.("people")
   // people.
 
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 10 and age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
 
    sc.stop()
  }
*/}
