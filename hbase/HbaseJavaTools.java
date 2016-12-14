package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.KeyValue;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by sunerhan on 2016/12/14.
 */
public class HbaseJavaTools {
    public static Configuration hbaseconfig = null;
    public static Connection    connection  = null;
    static {
        Configuration conf = new Configuration();
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("hbase.zookeeper.quorum", "hdd006,hdd007,hdd008");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseconfig = HBaseConfiguration.create(conf);
        try {
            connection = ConnectionFactory.createConnection(hbaseconfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws Exception{
        String tableName = "eda:tfj_prod_inst";
        System.out.println("test getRecordByList");
        List<String> rowkeys = new ArrayList();
        rowkeys.add("00000000000016724");
        rowkeys.add("00000000000007542");
        HbaseJavaTools.getRecordByList(tableName, rowkeys);

        System.out.println("test getRecordsByRange");
        HbaseJavaTools.getRecordsByRange(tableName, "00000000000007542", "00000000000024211");
    }
    
    /****
     * 根据rowkey范围使用scan查询所有数据
     * @param tableName
     */
    public static void getRecordsByRange(String tableName, String start, String stop)
        throws Exception {

        //创建table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        
        //Scan所有数据
        Scan scan = new Scan();
        scan.setStartRow(start.getBytes());
        scan.setStopRow(stop.getBytes());
        ResultScanner rss = table.getScanner(scan);
        
        for (Result r : rss) {
            System.out.println("\n row: " + new String(r.getRow()));
            for (KeyValue kv : r.raw()) {
                System.out.println("family.qualifer=>" + new String(kv.getFamily(), "utf-8") + "." + new String(kv.getQualifier(), "utf-8")
                                + "  value=>"  + new String(kv.getValue(), "utf-8"));
            }
        }
        rss.close();
    }
    
    /***
     * 根据主键rowKey列表查询数据
     */
    public static void getRecordByList(String tableName, List<String> rowkeys) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (String rowkey : rowkeys) {
            Get get = new Get(rowkey.getBytes()); //根据主键查询
            Result r = table.get(get);

            for (KeyValue kv : r.raw()) {
                //时间戳转换成日期格式
//                String timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss")
//                    .format(new Date(kv.getTimestamp()));
                //System.out.println("===:"+timestampFormat+"  ==timestamp: "+kv.getTimestamp());
//                System.out.println("\nKeyValue: " + kv);
//                System.out.println("key: " + kv.getKeyString());
                
                System.out.println("family.qualifer=>" + new String(kv.getFamily(), "utf-8") + "." + new String(kv.getQualifier(), "utf-8")
                                         + "  value=>" + new String(kv.getValue(), "utf-8"));
            }
        }
    }
    
    /**
     * 根据rowkey,一行中的某一列簇查询一条数据
     * get 'student','010','info'
     * student sid是010的info列簇（info:age,info:birthday）
     *
     * get 'student','010','info:age'
     * student sid是010的info:age列,quafilier是age
     */
    //public static void showOneRecordByRowKey_cloumn(String tableName,String rowkey,String column,String quafilier)
    public static void showOneRecordByRowKey_cloumn(String tableName, String rowkey, String column)  throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        
        Get get = new Get(rowkey.getBytes());
        get.addFamily(column.getBytes()); //根据主键查询某列簇
        //get.addColumn(Bytes.toBytes(column),Bytes.toBytes(quafilier)); ////根据主键查询某列簇中的quafilier列
        Result r = table.get(get);
        
        for (KeyValue kv : r.raw()) {
            System.out.println("KeyValue---" + kv);
            System.out.println("row=>" + new String(kv.getRow()));
            System.out.println("family=>" + new String(kv.getFamily(), "utf-8") + ": " + new String(kv.getValue(), "utf-8"));
            System.out.println("qualifier=>" + new String(kv.getQualifier()) + "\n");
            
        }
    }
    
    //（1）时间戳到时间的转换.单一的时间戳无法给出直观的解释。
    public String GetTimeByStamp(String timestamp) {
        long datatime = Long.parseLong(timestamp);
        Date date = new Date(datatime);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss");
        String timeresult = format.format(date);
        System.out.println("Time : " + timeresult);
        return timeresult;
        
    }
    
    //（2）时间到时间戳的转换。注意时间是字符串格式。字符串与时间的相互转换，此不赘述。
    public String GetStampByTime(String time) {
        String Stamp = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date;
        try {
            date = sdf.parse(time);
            Stamp = date.getTime() + "000";
            System.out.println(Stamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Stamp;
    }
}
