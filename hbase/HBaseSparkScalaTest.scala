package hbase

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes

object HBaseSparkScalaTest {
  def main(args: Array[String]) {
    var tool = new HbaseSparkScalaTools()
    tool.getRecordByRowkey("app1","00000000000016724")
//    println(999999)
//    tool.getRecordByRowkey("app2","00000000000007542")

    //    val scConf = new SparkConf().setAppName("hbasesparktest").setMaster("local")

    /*val tableName = "t1"
    val columnFamily1 = "f1"
    val column1 = "cell1"
    val column2 = "_0"*/

     val tableName = "NS_WH:PARTY"
    val columnFamily1 = "INFO"
    val column1 = "PARTY_ID"
    val column2 = "AREA_ID"

    //   从 /usr/hdp/2.4.0.0-169/hbase/conf/hbase-site.xml中读取
    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent","/hbase-secure")
    conf.set("hbase.zookeeper.quorum","hdd332,hdd333,hdd334,hnn024,hnn025")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hadoop.security.authentication" , "Kerberos" )
    conf.set("keytab.file" , "C:/Users/Downloads/hbase.keytab" )
    conf.set("kerberos.principal" , "hbase/1722.myip.domain@HADOOP.COM" )

   val connect = ConnectionFactory.createConnection(conf)
    val t1 = connect.getTable(TableName.valueOf(tableName))

    //查询数据
    val get = new Get("00000000000016724".getBytes)
    val getresult = t1.get(get)
   /* //指定想要查询的列簇列名
    val value = Bytes.toString(result.getValue(columnFamily1.getBytes,column.getBytes))*/
    getresult.raw.map(kv=> println("family.qualifer=>" + new String(kv.getFamily, "utf-8") + "." + new String(kv.getQualifier, "utf-8")
      + "  value=>" + new String(kv.getValue, "utf-8")))

    val scan = new Scan
    scan.setStartRow("".getBytes())
    scan setStopRow("".getBytes())
    val scanresult = t1.getScanner(scan)
    getresult.raw.map(kv=> println("family.qualifer=>" + new String(kv.getFamily, "utf-8") + "." + new String(kv.getQualifier, "utf-8")
      + "  value=>" + new String(kv.getValue, "utf-8")))
  }
}
