

import java.sql.DriverManager
 /**
  * JDBC/ODBC������SparkSQL��������һ��scala���룬��ѯ��t_ord_saleroder
  *����˿����� nohup hive --service metastore > metastore.log 2>&1 &
  * jobs
  * cd  /usr/spark/spark-2.1.0-bin-hadoop2.7/sbin
  * ./start-thriftserver.sh --master spark://192.168.62.128:7077 --executor-memory 1g
  * ���
  */
object JDBCofSparkSQL {
	def main(args: Array[String]) {
		Class.forName("org.apache.hive.jdbc.HiveDriver")
		val conn = DriverManager.getConnection("jdbc:hive2://192.168.62.128:10000/hive", "root", "yang123")
		try {
			val statement = conn.createStatement
					val rs = statement.executeQuery("select ordernumber,amount from t_ord_saleroder   where amount>3000")
					while (rs.next) {
						val ordernumber = rs.getString("ordernumber")
								val amount = rs.getString("amount")
								println("ordernumber = %s, amount = %s".format(ordernumber, amount))
					}
		} catch {
		case e: Exception => e.printStackTrace
		}
		conn.close
	}
}
/*
 * Could not open client transport with JDBC Uri: jdbc:hive2://192.168.62.128:10000/hive: java.net.ConnectException: Connection refused: connect
 * Ҫ��������spar��
 * 
 * ./spark-shell --master spark://192.168.62.128:7077 --executor-memory 1g
 */
