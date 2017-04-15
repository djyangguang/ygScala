
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
    object HiveTables {
      	def main(args :Array[String]){
      	  
      	  case class Record(key: Int, value: String)
    
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "/user/hive/warehouse";
    
    val spark = SparkSession
      .builder()
      //Caused by: java.lang.ClassCastException: cannot assign instance of scala.collection.immutable.List$SerializationProxy
      //to field org.apache.spark.rdd.RDD.org$apache$spark$rdd$RDD$$dependencies_ of type scala.collection.Seq in instance of org.apache.spark.rdd.MapPartitionsRDD
      //错误 就的用maseter(local)
      .master("local")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import spark.sql
    	val path = "hdfs://192.168.62.128:9000/laoyang/resources/kv1.txt"
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
   // 直接在服务器中执行： LOAD DATA LOCAL INPATH '/usr/spark/spark-2.1.0-bin-hadoop2.7/examples/src/main/resources/kv1.txt' INTO TABLE src;
    //sql("LOAD DATA LOCAL INPATH '"+path+"' INTO TABLE src")
    
    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...
    
    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+
    
    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    
    // The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...
    
    // You can also use DataFrames to create temporary views within a SparkSession.
  //  val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
//    val recotdsDf = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
//         recordsDF.createOrReplaceTempView("records")
    
    // Queries can then join DataFrame data with data stored in Hive.
   // sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // |  5| val_5|  5| val_5|
    // ...
      	}
}