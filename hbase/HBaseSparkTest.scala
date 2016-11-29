package hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object HBaseSparkTest {
  def main(args: Array[String]) {
    val scConf = new SparkConf().setAppName("hbasesparktest").setMaster("local")
    val sc = new SparkContext(scConf)

    val tableName = "t1"
    val columnFamily1 = "f1"
    val column1 = "cell1"
    val column2 = "_0"
    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
    conf.set("hbase.zookeeper.quorum","hdd006,hdd007,hdd008")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    println("9999999999999999999999999999999999999999999999")

    val usersRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //遍历输出
    usersRDD.foreach{ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue(columnFamily1.getBytes,column1.getBytes))
      println("Row key:"+key+" Name:"+name)
    }
    println(column1+",count:"+usersRDD.count())

    usersRDD.foreach{ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue(columnFamily1.getBytes,column2.getBytes))
      println("Row key:"+key+" Name:"+name)
    }
    println(column2+",count:"+usersRDD.count())

  /*  val connect = ConnectionFactory.createConnection(conf)
    val t1 = connect.getTable(TableName.valueOf(tableName))

    //查询数据
    val g = new Get(rowkey.getBytes)
    val result = t1.get(g)
    //指定想要查询的列簇列名
    val value = Bytes.toString(result.getValue(columnFamily1.getBytes,column.getBytes))
    println("content of 0001 :"+value)*/
  }
}
