package hbase

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sunerhan on 2016/12/16.
 */
object HbaseDataFrameTools {
  /*class MyRegistor extends KryoRegistrator{
    override def registerClasses(kryo: Kryo): Unit = {
      kryo.register(classOf[org.apache.hadoop.hbase.KeyValue])
      kryo.register(classOf[hbase.FFCSHbaseJavaRecord])
    }
  }*/
    val scConf = new SparkConf().setAppName("hbasesparktest")
    val sc = new SparkContext(scConf)
    val sqlContext= new SQLContext(sc)
    val tableName = "eda:tfj_prod_inst"

  def main(args: Array[String]) {
    /*System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "hbase.HbaseDataFrameTools.MyRegistor");*/

   /* val rowkeysets =  Seq("00000000000007542","00000000000016724","00000000000019154")
    val df = getRecordsBySetsDataFrame(tableName, Seq(rowkeysets),1)
    println(" count="+df.count())
    df.show()*/
   /* df.registerTempTable("temp")
    sqlContext.sql("select * from temp").foreach(x=>println)*/
//    df.printSchema()

    val rowkeyrange = Seq("00000000000007542", "00000000000016725","00000000000019153", "00000000000024211")
    var df = getRecordsByRangeDataFrame(tableName,rowkeyrange,2)
//    df.printSchema()
    println(" count="+df.count())
    df.show()
  }

  def getRecordsBySetsDataFrame(tableName: String, rowkeySets: Seq[Seq[String]],numOfPartitions: Int):org.apache.spark.sql.DataFrame={

    val rdd0 = sc.parallelize(rowkeySets, numOfPartitions)     // RDD[Seq[String]]
    // RDD[FFCSHbaseRecord]
    val rdd1 = rdd0.mapPartitions {
        (iter: Iterator[Seq[String]]) =>
          val splitKeysets = iter.next()
          assert(!iter.hasNext)
          HBaseScalaTools.getRecordBySet(tableName, splitKeysets)
      }
//    rdd1.foreach(x=>println("valuesize="+x.values.toString().split("_").size +" ,keysize= "+x.keySet.toString().split(" ").size))
//    rdd1.foreach(x=>println(x.values.toString()))
    val rdd2 = rdd1
      .map( x => {
        Row.fromSeq(
          x.values.toArray.reduceLeft((sum,i)=>sum.concat(i)).split("_"))} )
    val schema = StructType(rdd1.first().keySet.toList.map(fieldName => StructField(fieldName,StringType,true)))
//    rdd2.foreach(x=>println(x+" size="+x.size))
//    rdd2.foreach(y => println(y.size+"?="+schema.size))
    sqlContext.createDataFrame(rdd2,schema)
  }

  def getRecordsByRangeDataFrame(table: String, rowkeyRange: Seq[String],numOfPartitions: Int): org.apache.spark.sql.DataFrame={
    //    val rowkeys =  List("00000000000016724","00000000000007542")

    val rdd0 = sc.parallelize(rowkeyRange, numOfPartitions)
    val rdd1 = rdd0.mapPartitions {
      (iter:Iterator[String]) =>
        /*val splitKeyRange = iter.next()
        println(splitKeyRange)*/
        val start = iter.next()
        val stop = iter.next()
        HBaseScalaTools.getRecordsByRange(table, start, stop)
    }
    val rdd2 = rdd1
      .map( x => {
        Row.fromSeq(x.values.toArray.reduceLeft((sum,i)=>sum.concat(i)).split("_")) } )
    val schema = StructType(rdd1.first().keySet.toList.map(fieldName => StructField(fieldName,StringType,true)))
    sqlContext.createDataFrame(rdd2,schema)
  }
}
