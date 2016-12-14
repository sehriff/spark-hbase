package hbase

import org.apache.spark.{SparkContext, SparkConf}
import com.twitter.chill.Base64
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos

class HbaseSparkScalaTools {
  def getRecordByRowkey(appName:String,rowkey:String):String = {
    val scConf = new SparkConf().setAppName("hbasesparktest")
    val sc = new SparkContext(scConf)

    val tableName = "eda:tfj_prod_inst"
    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set("hbase.zookeeper.quorum", "hdd006,hdd007,hdd008")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    var scan = new Scan
    scan.setStartRow(rowkey.getBytes)
    scan.setStopRow(rowkey.getBytes)

    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val ScanToStrin: String = Base64.encodeBytes(proto.toByteArray)
    conf.set(TableInputFormat.SCAN, ScanToStrin)

    val usersRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    usersRDD.foreach {
      case (_, result) => {
        result.raw.map(kv=> println("family.qualifer=>" + new String(kv.getFamily, "utf-8") + "." + new String(kv.getQualifier, "utf-8")
                                          + "  value=>" + new String(kv.getValue, "utf-8")))
       /* var it = result.listCells().iterator()
        while(it.hasNext) {
          var family = Bytes.toString(it.next().getFamily)
          var column = Bytes.toString(it.next().getQualifier)
          println("key=" + column+" ,value="+Bytes.toString(result.getValue(family.getBytes,column.getBytes)))
        }*/
        }
    }
    return ""
  }
}
