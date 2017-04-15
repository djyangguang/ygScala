import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
/*Spark Streaming是一种近实时的流式计算模型，它将作业分解成一批一批的短小的批处理任务，
然后并行计算，具有可扩展，高容错，高吞吐，实时性高等一系列优点，
在某些场景可达到与Storm一样的处理程度或优于storm，也可以无缝集成多重日志收集工具或队列中转器，
比如常见的 kakfa，flume，redis，logstash等，计算完后的数据结果，也可以 
存储到各种存储系统中，如HDFS，数据库等，一张简单的数据流图如下
*/
object StreamingWordCount {
  def main(args: Array[String]) {
    //开本地线程两个处理
    val conf = new SparkConf().setMaster("spark://192.168.62.128:7077").setAppName("NetworkWordCount")
    //每隔10秒计算一批数据
    val ssc = new StreamingContext(conf, Seconds(10))
    //监控机器ip为192.168.1.187:9999端号的数据,注意必须是这个9999端号服务先启动nc -l 9999，否则会报错,但进程不会中断
    val lines = ssc.socketTextStream("192.168.62.128", 9999)
    //按空格切分输入数据
    val words = lines.flatMap(_.split(" "))
    //计算wordcount
    val pairs = words.map(word => (word, 1))
    //word ++
    val wordCounts = pairs.reduceByKey(_ + _)
    //排序结果集打印，先转成rdd，然后排序true升序，false降序，可以指定key和value排序_._1是key，_._2是value
    val sortResult=wordCounts.transform(rdd=>rdd.sortBy(_._2,false))
    sortResult.print()
    ssc.start()             // 开启计算
    ssc.awaitTermination()  // 阻塞等待计算
    //val nameHasUpperCase = name.exists()
  }
  def max (x:Int,y:Int) :Int ={
    if(x>y)
      x
    else
      y
  }

}