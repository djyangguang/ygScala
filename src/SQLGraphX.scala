import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.graphx._





object SQLGraphX {
  def main(args: Array[String]) {
    //������־
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    val warehouseLication ="/user/hive/warehouse"
      val spark = SparkSession
      .builder()
      //.master("spark://192.168.62.128:7077")
        .master("local")
      .appName("����")
       .config("spark.sql.warehouse.dir", warehouseLication)
       .enableHiveSupport()
       .getOrCreate()
    //ʹ��sparksql���ÿ��������������ͽ��
       import spark.implicits._
        import spark.sql
    sql("use hive")
    sql("SET spark.sql.shuffle.partitions=20")
    //ʹ��sparksql���ÿ��������������ͽ��
    sql("use hive")
    val verticesdata = spark.sql("select id, title from vertices")
    val edgesdata = spark.sql("select srcid,distid from edges")
 
    //װ�ض���ͱ�
    val vertices = verticesdata.rdd.map { case Row(id, title) => (id.toString.toLong, title.toString)}
    val edges = edgesdata.rdd.map { case Row(srcid, distid) => Edge(srcid.toString.toLong, distid.toString.toLong, 0)}
 
    //����ͼ
    val graph = Graph(vertices, edges, "").persist()
 
    //pageRank�㷨�����ʱ��ʹ����cache()����ǰ��persist��ʱ��ֻ��ʹ��MEMORY_ONLY
    println("**********************************************************")
    println("PageRank���㣬��ȡ���м�ֵ������")
    println("**********************************************************")
    val prGraph = graph.pageRank(0.001).cache()
 
    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }
 
    titleAndPrGraph.vertices.top(10) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.foreach(t => println(t._2._2 + ": " + t._2._1))
 
    spark.stop()
  }
}