import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object wordCount {
	def main(args :Array[String]){
		val conf = new SparkConf()//����SparkConf����
				conf.setAppName("Wow,My First Spark Programe")//����Ӧ�ó�������ƣ��ڳ������еļ�ؽ�����Կ�������
				conf.setMaster("spark://192.168.62.128:7077")//��ʱ�������ڱ������У�����Ҫ��װSpark��Ⱥ
				val sc = new SparkContext(conf)
				val lines = sc.textFile("hdfs://192.168.62.128:9000/laoyang/comment12031715.txt")
				val words = lines.flatMap{line => line.split(" ")}//��ÿһ�е��ַ������е��ʲ�ֲ��������еĲ�ֽ��ͨ��flat�ϲ���Ϊһ����ĵ��ʼ���
		    val pairs = words.map{word => (word,1)}
		    val wordCounts = pairs.reduceByKey(_+_)//����ͬ��Key������Value���ۼƣ�����Local��Reducer����ͬʱReduce��
				wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " +wordNumberPair._2))//���������д�ӡ�ý��

				sc.stop()//�ǵùرմ�����SparkContext����

















	}
}