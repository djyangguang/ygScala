import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object wordCount {
	def main(args :Array[String]){
		val conf = new SparkConf()//创建SparkConf对象
				conf.setAppName("Wow,My First Spark Programe")//设置应用程序的名称，在程序运行的监控界面可以看到名称
				conf.setMaster("spark://192.168.62.128:7077")//此时，程序在本地运行，不需要安装Spark集群
				val sc = new SparkContext(conf)
				val lines = sc.textFile("hdfs://192.168.62.128:9000/laoyang/comment12031715.txt")
				val words = lines.flatMap{line => line.split(" ")}//对每一行的字符串进行单词拆分并把所有行的拆分结果通过flat合并成为一个大的单词集合
		    val pairs = words.map{word => (word,1)}
		    val wordCounts = pairs.reduceByKey(_+_)//对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
				wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : " +wordNumberPair._2))//在命令行中打印该结果

				sc.stop()//记得关闭创建的SparkContext对象

















	}
}