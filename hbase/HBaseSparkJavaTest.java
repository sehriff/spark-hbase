package hbase;

import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by sunerhan on 2016/11/24.
 */
public class HBaseSparkJavaTest {
    public static void main(String[] args) throws IOException {
        SparkConf scConf = new SparkConf().setAppName("hbasesparktest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(scConf);
        
        String tableName = "t1";
        String columnFamily1 = "f1";
        String column = "cell1";
//        String column = "_0";

        Configuration conf = HBaseConfiguration.create();
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("hbase.zookeeper.quorum", "hdd006,hdd007,hdd008");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily1));
        scan.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes(column));
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String ScanToStrin = Base64.encodeBytes(proto.toByteArray());
        conf.set(TableInputFormat.SCAN,ScanToStrin);

        System.out.println("9999999999999999");

        JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                sc.newAPIHadoopRDD(conf,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class);
        myRDD.cache().foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>() {
            @Override
            public void call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {

            }
        });
        System.out.println("count==="+myRDD.count());
    }
}
