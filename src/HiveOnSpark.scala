
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession
 
object HiveOnSpark {
  case class Record(key: Int, value: String)
 
  def main(args: Array[String]) {/*
  	//version 1.2.1 µÄÐ´·¨ 
    val sparkConf = new SparkConf().setAppName("HiveOnSpark")
    sparkConf.setMaster("spark://192.168.62.128:7077");
    val sc = new SparkContext(sparkConf)
 
    val hiveContext = new HiveContext(sc)
    import hiveContext._
 
    sql("use hive")
    sql("select c.theyear,count(distinct a.ordernumber),sum(b.amount) from tbStock a join tbStockDetail b on a.ordernumber=b.ordernumber join tbDate  c on a.dateid=c.dateid group by c.theyear order by c.theyear")
      .collect().foreach(println)
 
    sc.stop()
  */
    val warehouseLocation = "/user/hive/warehouse";
    val spark =SparkSession
        .builder()
        .master("spark://192.168.62.128:7077")
        .appName("laoyang HiveONSpark")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()
        import spark.implicits._
        import spark.sql
        sql("use hive")
      //  sql("SET spark.sql.shuffle.partitions=20")
        sql("select c.theyear,count(distinct a.ordernumber),sum(b.amount) from t_ord_btock a join t_ord_saleroder b on a.ordernumber=b.ordernumber join t_ord_date  c on a.dateid=c.dateid group by c.theyear order by c.theyear")
        .collect().foreach(println)   
        spark.stop()
        /**
         * 	17/03/06 11:06:44 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 6, 192.168.62.132, executor 1, partition 0, NODE_LOCAL, 5879 bytes)
						17/03/06 11:06:44 INFO TaskSetManager: Starting task 1.0 in stage 3.0 (TID 7, 192.168.62.131, executor 0, partition 1, NODE_LOCAL, 5879 bytes)
						17/03/06 11:06:44 INFO TaskSetManager: Starting task 2.0 in stage 3.0 (TID 8, 192.168.62.131, executor 0, partition 2, NODE_LOCAL, 5879 bytes)
						17/03/06 11:06:44 INFO TaskSetManager: Starting task 3.0 in stage 3.0 (TID 9, 192.168.62.131, executor 0, partition 3, NODE_LOCAL, 5879 bytes)
						17/03/06 11:06:44 INFO TaskSetManager: Starting task 4.0 in stage 3.0 (TID 10, 192.168.62.131, executor 0, partition 4, NODE_LOCAL, 5879 bytes)
						[2006,3772,13670416]
						[2007,4885,16711974]
						[2008,4861,14670698]
						[2009,2619,6322137]
						[2010,94,210924]
         */
  }
}
