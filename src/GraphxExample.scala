
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
 
object GraphXExample {
  def main(args: Array[String]) {
    //������־
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //�������л���
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)
 
    //���ö���ͱߣ�ע�ⶥ��ͱ߶�����Ԫ�鶨���Array
    //���������������VD:(String,Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //�ߵ���������ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
 
    //����vertexRDD��edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
 
    //����ͼGraph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
 
    //***********************************************************************************
    //***************************  ͼ������    ****************************************
    //**********************************************************************************         println("***********************************************")
    println("������ʾ")
    println("**********************************************************")
    println("�ҳ�ͼ���������30�Ķ��㣺")
    graph.vertices.filter { case (id, (name, age)) => age > 30}.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }
 
    //�߲������ҳ�ͼ�����Դ���5�ı�
    println("�ҳ�ͼ�����Դ���5�ıߣ�")
    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println
 
    //triplets������((srcId, srcAttr), (dstId, dstAttr), attr)
    println("�г�������>5��tripltes��")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println
 
    //Degrees����
    println("�ҳ�ͼ�����ĳ��ȡ���ȡ�������")
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))
    println
   
    //***********************************************************************************
    //***************************  ת������    ****************************************
    //**********************************************************************************  
    println("**********************************************************")
    println("ת������")
    println("**********************************************************")
    println("�����ת������������age + 10��")
    graph.mapVertices{ case (id, (name, age)) => (id, (name, age+10))}.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println
    println("�ߵ�ת���������ߵ�����*2��")
    graph.mapEdges(e=>e.attr*2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println
  
      //***********************************************************************************
    //***************************  �ṹ����    ****************************************
    //**********************************************************************************  
    println("**********************************************************")
    println("�ṹ����")
    println("**********************************************************")
    println("�������>30����ͼ��")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
    println("��ͼ���ж��㣺")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println
    println("��ͼ���бߣ�")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println
 
   
      //***********************************************************************************
    //***************************  ���Ӳ���    ****************************************
    //**********************************************************************************  
    println("**********************************************************")
    println("���Ӳ���")
    println("**********************************************************")
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
 
    //����һ����ͼ������VD����������ΪUser������graph������ת��
    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0)}
 
    //initialUserGraph��inDegrees��outDegrees��RDD���������ӣ����޸�initialUserGraph��inDegֵ��outDegֵ
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg,outDegOpt.getOrElse(0))
    }
 
    println("����ͼ�����ԣ�")
userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
    println
 
    println("���Ⱥ������ͬ����Ա��")
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }
    println
 
      //***********************************************************************************
    //***************************  �ۺϲ���    ****************************************
    //**********************************************************************************  
   
 
     //***********************************************************************************
    //***************************  ʵ�ò���    ****************************************
    //**********************************************************************************
    println("**********************************************************")
    println("�ۺϲ���")
    println("**********************************************************")
    println("�ҳ�5�����������̣�")
    val sourceId: VertexId = 5L // ����Դ��
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist),
      triplet => {  // ����Ȩ��
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b) // ��̾���
    )
    println(sssp.vertices.collect.mkString("\n"))
 
    sc.stop()
  }
}