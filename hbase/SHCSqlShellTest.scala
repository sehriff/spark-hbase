package hbase

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source
import org.json4s.jackson.JsonMethods._

/**
 * Created by sunerhan on 2016/12/22.
 */
object SHCSqlShellTest {
  //从catalog文件中读取
  /*def cat1 =
  s"""{
     |"table":{"namespace":"default", "name":"t2"},
     |"rowkey":"key:CELL",
     |"columns":{
     |"row":{"cf":"rowkey", "col":"key", "type":"string", "length":"10"},
     |"cell1":{"cf":"f2", "col":"CELL", "type":"string"}
     |}
     |}
     """.stripMargin

  def cat =
    s"""{
       |"table":{"namespace":"default", "name":"t3"},
       |"rowkey":"key:CELL",
       |"columns":{
       |"row":{"cf":"rowkey", "col":"key", "type":"string", "length":"10"},
       |"cell1":{"cf":"f3", "col":"CELL", "type":"string"}
       |}
       |}
     """.stripMargin*/


  //要执行的sql语句,从sqlfile中读取
  /*"select * from table1"
  "select * from table2"
  "select * from table2 join table1 on table2.cell1=table1.cell1 and table2.row > 'rowkey1' and table2.row < 'rowkey33'"
  "select cell1,count(1) from table2 where table2.row > 'rowkey1' and table2.row < 'rowkey33' group by cell1"*/

  def main(args: Array[String]) {
    val sourcepath= "/home/ffcs/sparkhbasetest/"
//    val sourcepath= "C:\\Users\\sunerhan\\Documents\\IdeaProj\\edw_spark\\src\\main\\scala\\"
    val sqlfilepath= sourcepath+"sqlfile.txt"
    val catalogpath= sourcepath+"catalogs.txt"

    val sparkConf = new SparkConf().setAppName("CompositeKeyTest")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      hiveContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }
    //根据catalog获取表名,用于sqlContext中注册
    def getTableName(catalogString :String): String = {
      val map= parse(catalogString).values.asInstanceOf[Map[String,_]]
      val tableMeta = map.get("table").get.asInstanceOf[Map[String, _]]
      val tName = tableMeta.get("name").get.asInstanceOf[String]
      "hbase"+tName
    }

    val catalogs = Source.fromFile(catalogpath).getLines().reduceLeft((left,right)=>left.concat(right.trim)).split(";",-1)
    val catMap = new scala.collection.mutable.LinkedHashMap[String, String]
    catalogs.foreach(m => {
      if (m!=""&&m.size>0) {
        val tbName = getTableName(m)
        catMap.put(tbName,m.toString)
      }
    })

    catalogs.foreach(m => {
      if (m!=""&&m.size>0) {
        val tbName = getTableName(m)
        val df = withCatalog(m)
        df.registerTempTable(tbName)
      }
    })

    def getCatalogByName(name:String): String ={
      catMap(name)
    }

    def populate(sourceSqlString:String,destinateCat:String): Unit = {
//      println(getCatalogByName(destinateCat))
        hiveContext.sql(sourceSqlString).write.options(
        Map(HBaseTableCatalog.tableCatalog -> getCatalogByName(destinateCat), HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }

  val sqls = Source.fromFile(sqlfilepath).getLines().reduceLeft((left,right)=>" ".concat(left).concat(right.trim)).split(";",-1)
  sqls.foreach(m=>if (m!=""&&m.size>0) {
  println(" ready to execute sql "+m.toString + " end of sql")
  if(m.contains("insert")){
    var destCat=""
    var sourceSqlString=""
    m.split("select").foreach(s=>{
        if(s.contains("into"))
          s.split("into").
            foreach(m=>
              if(!m.contains("insert"))
                destCat=m)
        else
          sourceSqlString = "select"+s.toString
    })
    if(!destCat.equals("")&&(!sourceSqlString.equals(""))) {
//      println(" sourceSqlString= "+sourceSqlString+" destcat= "+destCat)
      populate(sourceSqlString,destCat.trim)
    }
  }else{
    hiveContext.sql(m.toString).show(100)
  }
})
}
}
