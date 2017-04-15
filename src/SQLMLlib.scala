
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors










 /**
  * ����
  * ������ʵ��Ӧ���зǳ��ձ飬����Կͻ����з��ࡢ�Ե��̽��з���ȵȣ��Բ�ͬ����ȡ��ͬ�Ĳ��ԣ�
  * ������Ч�Ľ�����ҵ��Ӫ�˳ɱ����������롣����ѧϰ�еľ������һ�ָ��ݲ�ͬ���������ݣ�
  * ����û�ָ������������������ݷֳɼ�����ķ�����
  * �����������������۽���������������ݣ����о��࣬�ֳ�3���ȼ��ĵ��̡�
  */
object SQLMLlib {
	def main(args: Array[String]) {  
		//���β���Ҫ����־��ʾ���ն���
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
		val warehouseLication ="/user/hive/warehouse"
		val spark = SparkSession
		.builder()
		//Caused by: java.lang.ClassCastException: cannot assign instance of scala.collection.immutable.List$SerializationProxy
		//to field org.apache.spark.rdd.RDD.org$apache$spark$rdd$RDD$$dependencies_ of type scala.collection.Seq in instance of org.apache.spark.rdd.MapPartitionsRDD
		//���� �͵���maseter(local)
		//.master("local")
		.master("spark://192.168.62.128:7077")
		.appName("laoyangSQLMllib")
		.config("spark.sql.warehouse.dir", warehouseLication)
		.enableHiveSupport()
		.getOrCreate()

		//ʹ��sparksql���ÿ���ͻ������������ͽ��
		import spark.implicits._
		import spark.sql
		spark.sql("use hive")
		spark.sql("SET spark.sql.shuffle.partitions=20")
		//val sqldata = spark.sql("select a.locationid, sum(b.qty) totalqty,sum(b.amount) totalamount from t_ord_btock a join t_ord_saleroder b on a.ordernumber=b.ordernumber group by a.locationid")
		val sqldata=spark.sql("select customername locationid,sum(amount) totalamount,sum(price) totalqty  from t_ord_saleroder03 where amount>0 and price>0   group by customername");
		sqldata.show()
		//����ѯ����ת��������		
		//   //data.show() //д����ͱ���Caused by: scala.MatchError: [ZM,30513,4267355] (of class org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema)
		val parsedData = sqldata.rdd.map{ case Row(locationid,totalqty, totalamount) =>  // s"totalqty: $totalqty, totalamount: $totalamount"       
		val features = Array[Double](totalqty.toString().toDouble, totalamount.toString().toDouble)  
		//features.foreach { x => ??? }
		  Vectors.dense(features)   
		}
		
		System.out.println("===================>");

		
		//val paresData1= sqldata.map(func, encoder) 
		//�����ݼ����࣬3���࣬20�ε������γ�����ģ��
		//ע�������ʹ�����õ�partition��20
		val numClusters = 5 //�����������������۽���������������ݣ����о��࣬�ֳ�3���ȼ��Ŀͻ�
		val numIterations = 20
		//���������ݼ����о���ѵ������������᷵�� KMeansModel ��ʵ����
		//Ȼ������Ҳ����ʹ�� KMeansModel.predict �������µ����ݵ�������������Ԥ�⣬���Ƿǳ�ʵ�õĹ��ܡ�
		val model = KMeans.train(parsedData, numClusters, numIterations)
		println("=========����ģ�͵����ĵ�===========>")
		for (c<-model.clusterCenters){
		  println(""+c.toString())
		}
		val cost = model.computeCost(parsedData);
		println("===========ʹ�����ƽ��֮������������ģ��====="+cost)
		// ʹ��ģ�Ͳ��Ե�������
  println(" ����50000 60000 is ���Ե�������:" + model.predict(Vectors.dense("50000,60000".split(',').map(_.toDouble))))
 // println("Vectors 0.25 0.25 0.25 is belongs to clusters:" + model.predict(Vectors.dense("0.25 0.25 0.25".split(' ').map(_.toDouble))))
 // println("Vectors 8 8 8 is belongs to clusters:" + model.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))
		//��ģ�ͶԶ�������ݽ��з��࣬�����
		//����partitionû���ã����Ϊ200��С�ļ�������ʹ��c �ϲ����ص�����
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
