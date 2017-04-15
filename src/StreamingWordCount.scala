import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
/*Spark Streaming��һ�ֽ�ʵʱ����ʽ����ģ�ͣ�������ҵ�ֽ��һ��һ���Ķ�С������������
Ȼ���м��㣬���п���չ�����ݴ������£�ʵʱ�Ըߵ�һϵ���ŵ㣬
��ĳЩ�����ɴﵽ��Stormһ���Ĵ���̶Ȼ�����storm��Ҳ�����޷켯�ɶ�����־�ռ����߻������ת����
���糣���� kakfa��flume��redis��logstash�ȣ������������ݽ����Ҳ���� 
�洢�����ִ洢ϵͳ�У���HDFS�����ݿ�ȣ�һ�ż򵥵�������ͼ����
*/
object StreamingWordCount {
  def main(args: Array[String]) {
    //�������߳���������
    val conf = new SparkConf().setMaster("spark://192.168.62.128:7077").setAppName("NetworkWordCount")
    //ÿ��10�����һ������
    val ssc = new StreamingContext(conf, Seconds(10))
    //��ػ���ipΪ192.168.1.187:9999�˺ŵ�����,ע����������9999�˺ŷ���������nc -l 9999������ᱨ��,�����̲����ж�
    val lines = ssc.socketTextStream("192.168.62.128", 9999)
    //���ո��з���������
    val words = lines.flatMap(_.split(" "))
    //����wordcount
    val pairs = words.map(word => (word, 1))
    //word ++
    val wordCounts = pairs.reduceByKey(_ + _)
    //����������ӡ����ת��rdd��Ȼ������true����false���򣬿���ָ��key��value����_._1��key��_._2��value
    val sortResult=wordCounts.transform(rdd=>rdd.sortBy(_._2,false))
    sortResult.print()
    ssc.start()             // ��������
    ssc.awaitTermination()  // �����ȴ�����
    //val nameHasUpperCase = name.exists()
  }
  def max (x:Int,y:Int) :Int ={
    if(x>y)
      x
    else
      y
  }

}