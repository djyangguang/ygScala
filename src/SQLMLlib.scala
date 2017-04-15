
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors










 /**
  * 聚类
  * 分类在实际应用中非常普遍，比如对客户进行分类、对店铺进行分类等等，对不同类别采取不同的策略，
  * 可以有效的降低企业的营运成本、增加收入。机器学习中的聚类就是一种根据不同的特征数据，
  * 结合用户指定的类别数量，将数据分成几个类的方法。
  * 按照销售数量和销售金额这两个特征数据，进行聚类，分出3个等级的店铺。
  */
object SQLMLlib {
	def main(args: Array[String]) {  
		//屏蔽不必要的日志显示在终端上
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		val warehouseLication ="/user/hive/warehouse"
		val spark = SparkSession
		.builder()
		//Caused by: java.lang.ClassCastException: cannot assign instance of scala.collection.immutable.List$SerializationProxy
		//to field org.apache.spark.rdd.RDD.org$apache$spark$rdd$RDD$$dependencies_ of type scala.collection.Seq in instance of org.apache.spark.rdd.MapPartitionsRDD
		//错误 就的用maseter(local)
		//.master("local")
		.master("spark://192.168.62.128:7077")
		.appName("laoyangSQLMllib")
		.config("spark.sql.warehouse.dir", warehouseLication)
		.enableHiveSupport()
		.getOrCreate()

		//使用sparksql查出每个客户的销售数量和金额
		import spark.implicits._
		import spark.sql
		spark.sql("use hive")
		spark.sql("SET spark.sql.shuffle.partitions=20")
		//val sqldata = spark.sql("select a.locationid, sum(b.qty) totalqty,sum(b.amount) totalamount from t_ord_btock a join t_ord_saleroder b on a.ordernumber=b.ordernumber group by a.locationid")
		val sqldata=spark.sql("select customername locationid,sum(amount) totalamount,sum(price) totalqty  from t_ord_saleroder03 where amount>0 and price>0   group by customername");
		sqldata.show()
		//将查询数据转换成向量		
		//   //data.show() //写这个就报错Caused by: scala.MatchError: [ZM,30513,4267355] (of class org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema)
		val parsedData = sqldata.rdd.map{ case Row(locationid,totalqty, totalamount) =>  // s"totalqty: $totalqty, totalamount: $totalamount"       
		val features = Array[Double](totalqty.toString().toDouble, totalamount.toString().toDouble)  
		//features.foreach { x => ??? }
		  Vectors.dense(features)   
		}
		
		System.out.println("===================>");

		
		//val paresData1= sqldata.map(func, encoder) 
		//对数据集聚类，3个类，20次迭代，形成数据模型
		//注意这里会使用设置的partition数20
		val numClusters = 5 //按照销售数量和销售金额这两个特征数据，进行聚类，分出3个等级的客户
		val numIterations = 20
		//方法对数据集进行聚类训练，这个方法会返回 KMeansModel 类实例，
		//然后我们也可以使用 KMeansModel.predict 方法对新的数据点进行所属聚类的预测，这是非常实用的功能。
		val model = KMeans.train(parsedData, numClusters, numIterations)
		println("=========数据模型的中心点===========>")
		for (c<-model.clusterCenters){
		  println(""+c.toString())
		}
		val cost = model.computeCost(parsedData);
		println("===========使用误差平方之和来评估数据模型====="+cost)
		// 使用模型测试单点数据
  println(" 向量50000 60000 is 测试单点数据:" + model.predict(Vectors.dense("50000,60000".split(',').map(_.toDouble))))
 // println("Vectors 0.25 0.25 0.25 is belongs to clusters:" + model.predict(Vectors.dense("0.25 0.25 0.25".split(' ').map(_.toDouble))))
 // println("Vectors 8 8 8 is belongs to clusters:" + model.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))
		//用模型对读入的数据进行分类，并输出
		//由于partition没设置，输出为200个小文件，可以使用c 合并下载到本地
		val result2 = sqldata.rdd.map {     
		case Row(locationid, totalqty, totalamount) =>   
    		val features = Array[Double](totalqty.toString.toDouble, totalamount.toString. toDouble) 
    		val linevectore = Vectors.dense(features)      
    		//val prediction = model.predict(linevectore)    result2 )  
    		val prediction = model.predict(linevectore)
    		locationid + " " + totalqty + " " + totalamount + " " + prediction 
		}.saveAsTextFile(args(0)) 
		//.saveAsTextFile("D:\\output01") 
	//	System.out.print(prediction);
		//.saveAsTextFile(args(0)) 
		println("========end ======")
		spark.stop()
	}
}
