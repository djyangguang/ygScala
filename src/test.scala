package laoyangSpark;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * ʹ��Scala�������ز��Ե�Spark WordCount����
 * ����ע���������������ֵĴ�������Ƶ
 */

object test {
  def main(args : Array[String]){
    /**
       * ��1��������Spark�����ö���SparkConf������Spark���������ʱ��������Ϣ��
       * ����˵ͨ��setMaster�����ó���Ҫ���ӵ�Spark��Ⱥ��Master��URL,�������
       * Ϊlocal�������Spark�����ڱ������У��ر��ʺ��ڻ������������ǳ������
       * ֻ��1G���ڴ棩�ĳ�ѧ��       * 
       */
    val conf = new SparkConf()//����SparkConf����
    conf.setAppName("Wow,My First Spark Programe")//����Ӧ�ó�������ƣ��ڳ������еļ�ؽ�����Կ�������
    conf.setMaster("local")//��ʱ�������ڱ������У�����Ҫ��װSpark��Ⱥ

    /**
       * ��2��������SparkContext����
       * SparkContext��Spark�������й��ܵ�Ψһ��ڣ������ǲ���Scala��Java��Python��R�ȶ�������һ��SparkContext
       * SparkContext�������ã���ʼ��SparkӦ�ó�����������Ҫ�ĺ������������DAGScheduler��TaskScheduler��SchedulerBackend
       * ͬʱ���Ḻ��Spark������Masterע������
       * SparkContext������SparkӦ�ó�������Ϊ������Ҫ��һ������
       */
    val sc = new SparkContext(conf)//����SparkContext����ͨ������SparkConfʵ��������Spark���еľ��������������Ϣ

     /**
       * ��3�������ݾ����������Դ��HDFS��HBase��Local FS��DB��S3�ȣ�ͨ��SparkContext������RDD
       * RDD�Ĵ������������ַ�ʽ�������ⲿ��������Դ������HDFS��������Scala���ϡ���������RDD����
       * ���ݻᱻRDD���ֳ�Ϊһϵ�е�Partitions�����䵽ÿ��Partition����������һ��Task�Ĵ�����
       */
    val lines = sc.textFile("/Users//xxm//Documents//soft//spark-1.5.2-bin-hadoop2.6//README.md",1)//��ȡ�����ļ�������Ϊһ��Partion
/**
       * ��4�����Գ�ʼ��RDD����Transformation����Ĵ�������map��filter�ȸ߽׺����ȵı�̣������о�������ݼ���
       *    ��4.1������ÿһ�е��ַ�����ֳɵ����ĵ���
       */

    val words = lines.flatMap{line => line.split(" ")}//��ÿһ�е��ַ������е��ʲ�ֲ��������еĲ�ֽ��ͨ��flat�ϲ���Ϊһ����ĵ��ʼ���

 /**
       * ��4�����Գ�ʼ��RDD����Transformation����Ĵ�������map��filter�ȸ߽׺����ȵı�̣������о�������ݼ���
       *    ��4.2�����ڵ��ʲ�ֵĻ����϶�ÿ������ʵ������Ϊ1��Ҳ����word => (word, 1)
       */

    val pairs = words.map{word => (word,1)}

    /**
       * ��4�����Գ�ʼ��RDD����Transformation����Ĵ�������map��filter�ȸ߽׺����ȵı�̣������о�������ݼ���
       *    ��4.3������ÿ������ʵ������Ϊ1����֮��ͳ��ÿ���������ļ��г��ֵ��ܴ���
       */
    val wordCounts = pairs.reduceByKey(_+_)//����ͬ��Key������Value���ۼƣ�����Local��Reducer����ͬʱReduce��

    wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " +wordNumberPair._2))//���������д�ӡ�ý��

    sc.stop()//�ǵùرմ�����SparkContext����
  }
}