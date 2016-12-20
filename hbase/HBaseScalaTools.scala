package hbase

import java.util

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.spark.{SparkConf, SparkContext}

object HBaseScalaTools {
  val conf = HBaseConfiguration.create()
  conf.set("zookeeper.znode.parent", "/hbase-unsecure")
  conf.set("hbase.zookeeper.quorum", "hdd006,hdd007,hdd008")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  /* conf.set("hadoop.security.authentication" , "Kerberos" )
    conf.set("keytab.file" , "C:/Users/Downloads/hbase.keytab" )
    conf.set("kerberos.principal" , "hbase/1722.myip.domain@HADOOP.COM" )*/
  val connect = ConnectionFactory.createConnection(conf)

  def main(args: Array[String]) {
    val tableName: String = "eda:tfj_prod_inst"
    //   从 /usr/hdp/2.4.0.0-169/hbase/conf/hbase-site.xml中读取
    var iter1 = getRecordsByRange(tableName, "00000000000007542", "00000000000024211")
    while (iter1.hasNext)
      println(iter1.next())

    println("88888888888888888888888888888888888888888888888888888888888")

    val list = List("00000000000016724", "00000000000007542")
    var iter2 = getRecordBySet(tableName, list)
    while (iter2.hasNext)
      println(iter2.next())
  }

  /**
   * 根据rowke范围查询Hbase表
   * @param table : Hbase表名
   * @param start : rowkey起始值
   * @param stop : rowkey终止值
   * @return : Iterator[FFCSHbaseRecord]
   */
  def getRecordsByRange(table: String, start: String, stop: String): Iterator[scala.collection.mutable.LinkedHashMap[String, String]] = {
    println(" start= "+start+" ,stop= "+stop)
    val t1 = connect.getTable(TableName.valueOf(table))
    val scan = new Scan
    scan.setStartRow(start.getBytes())
    scan.setStopRow(stop.getBytes())
    val scanresult = t1.getScanner(scan)
    val scanResultIter = scanresult.iterator()

    new Iterator[scala.collection.mutable.LinkedHashMap[String, String]] {
      override def hasNext: Boolean = {
        scanResultIter.hasNext
      }

      override def next(): scala.collection.mutable.LinkedHashMap[String, String] = {
        val scanCellIter = scanResultIter.next().listCells().iterator()
        val kvs = new scala.collection.mutable.LinkedHashMap[String, String]
        while (scanCellIter.hasNext) {
          val kv = KeyValueUtil.copyToNewKeyValue(scanCellIter.next())
          val key = new String(CellUtil.cloneFamily(kv), "utf-8") + "." + new String(CellUtil.cloneQualifier(kv), "utf-8")
          val value =  new String(CellUtil.cloneValue(kv), "utf-8")
          kvs.put(key,value+"_")
        }
//        new FFCSHbaseJavaRecord(new String(CellUtil.cloneRow(kvs.get(0)), "utf-8"), kvs)
        kvs
      }

      /*while (scanResultIter.hasNext) {
      var kvs = new util.ArrayList[KeyValue]()
      val scanCellIter = scanResultIter.next().listCells().iterator()
      while (scanCellIter.hasNext) {
        val kv = KeyValueUtil.copyToNewKeyValue(scanCellIter.next())
        kvs.add(kv)
        /*println(
          "rowkey=>"+new String(CellUtil.cloneRow(kv),"utf-8")
          + "family.qualifer=>" + new String(CellUtil.cloneFamily(kv), "utf-8") + "." + new String(CellUtil.cloneQualifier(kv), "utf-8")
          + "  value=>" + new String(CellUtil.cloneValue(kv), "utf-8"))*/
      }
      records.add(new FFCSHbaseRecord(new String(CellUtil.cloneRow(kvs.get(0)), "utf-8"), kvs))
    }*/
    }
  }

  /**
   * 查询多个rowkey,返回多条hbase记录
   * @param table : Hbase表名
   * @param rowkeys : 多个rowkey值
   * @return : Iterator[FFCSHbaseRecord]
   */
  def getRecordBySet(table: String, rowkeys: Seq[String]): Iterator[scala.collection.mutable.LinkedHashMap[String, String]] = {
//    val rowkeysArray = rowkeys.toArray
    val t1 = connect.getTable(TableName.valueOf(table))
//    val records = new ArrayBuffer[FFCSHbaseRecord]
    val rowkeysIter = rowkeys.iterator


      /* //指定想要查询的列簇列名
       val value = Bytes.toString(result.getValue(columnFamily1.getBytes,column.getBytes))*/
      /* getresult.raw.map(kv=> println("family.qualifer=>" + new String(kv.getFamily, "utf-8") + "." + new String(kv.getQualifier, "utf-8")
         + "  value=>" + new String(kv.getValue, "utf-8")))*/

//      records += new FFCSHbaseRecord(rowkey.toString, kvs)

      new Iterator[scala.collection.mutable.LinkedHashMap[String, String]] {
        override def hasNext: Boolean = {
          rowkeysIter.hasNext
        }

        override def next(): scala.collection.mutable.LinkedHashMap[String, String] = {
          val rowkey = rowkeysIter.next()
          val kvs = new scala.collection.mutable.LinkedHashMap[String, String]
          println(" rowkey= "+rowkey)
          val get = new Get(rowkey.toString.getBytes)
          val getresult = t1.get(get)
          val getIter = getresult.listCells().iterator()
          while (getIter.hasNext) {
            val kv = KeyValueUtil.copyToNewKeyValue(getIter.next())
            var key = new String(CellUtil.cloneFamily(kv), "utf-8") + "." + new String(CellUtil.cloneQualifier(kv), "utf-8")
            val value =  new String(CellUtil.cloneValue(kv), "utf-8")
            kvs.put(key, value+"_")
            /*println(
              "rowkey=>"+new String(CellUtil.cloneRow(kv),"utf-8")
                + "family.qualifer=>" + new String(CellUtil.cloneFamily(kv), "utf-8") + "." + new String(CellUtil.cloneQualifier(kv), "utf-8")
                + "  value=>" + new String(CellUtil.cloneValue(kv), "utf-8"))*/
          }
//          new FFCSHbaseJavaRecord(rowkey.toString, kvs)
          kvs
        }
      }
      //    records.toIterator
    }
}