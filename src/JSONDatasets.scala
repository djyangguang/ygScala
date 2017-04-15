
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession

object JSONDatasets {
  
  
	def main(args: Array[String]) {
	  case class Record(key: Int, value: String)
		val warehouseLocation = "/user/hive/warehouse";
		val spark =SparkSession
				.builder()
				.master("spark://192.168.62.128:7077")
				.appName("laoyang JSONDatasets")
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.enableHiveSupport()
				.getOrCreate()
				// A JSON dataset is pointed to by path.
				// The path can be either a single text file or a directory storing text files
				val path = "hdfs://192.168.62.128:9000/laoyang/resources/people.json"
				val peopleDF = spark.read.json(path)

				// The inferred schema can be visualized using the printSchema() method
				peopleDF.printSchema()
				// root
				//  |-- age: long (nullable = true)
				//  |-- name: string (nullable = true)

				// Creates a temporary view using the DataFrame
				peopleDF.createOrReplaceTempView("people")

				// SQL statements can be run by using the sql methods provided by spark
				val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
				teenagerNamesDF.show()
				// +------+
				// |  name|
				// +------+
				// |Justin|
				// +------+

				// Alternatively, a DataFrame can be created for a JSON dataset represented by
				// an RDD[String] storing one JSON object per string
				val otherPeopleRDD = spark.sparkContext.makeRDD(
						"""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
				val otherPeople = spark.read.json(otherPeopleRDD)
				otherPeople.show()
				// +---------------+----+
				// |        address|name|
				// +---------------+----+
				// |[Columbus,Ohio]| Yin|
				// +---------------+----+
	}
}