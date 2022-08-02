package com.yiban.spark3.sql.scala.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, rank, row_number}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Encoders, SparkSession, functions}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.util.Properties
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.language.postfixOps

class PushDownTest {
  val user = "root"
  val password = "120653"
  val url = "jdbc:mysql://192.168.26.11:30053/linkflow?useUnicode=yes&characterEncoding=UTF-8&useLegacyDatetimeCode=false&serverTimezone=GMT%2b8&zeroDateTimeBehavior=convertToNull&useSSL=false&cachePrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048&useServerPrepStmts=true&useLocalSessionState=true&useLocalTransactionState=true&rewriteBatchedStatements=true&cacheResultSetMetadata=true&cacheServerConfiguration=true&elideSetAutoCommits=true&maintainTimeStats=false"
  val fullUrl = s"jdbc:mysql://localhost:3306/test?user=${user}&password=${password}&allowPublicKeyRetrieval=true&useSSL=false"
  val driver = "com.mysql.jdbc.Driver"

  @BeforeEach
  def init(): Unit = {
    val logger: Logger = Logger.getLogger("org.apache.spark")
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  /**
   * df.filter('i > 3).groupBy('j).agg(sum($"i")).where(sum('i) > 10)
   *
   * After Aggregate Push Down, it has the following Optimized Logical Plan and Physical Plan:
   *
   * == Optimized Logical Plan ==
   * DataSourceV2Relation (source=AdvancedDataSourceV2,schema=[i#0 int, j#1 int],filters=[isnotnull(i#0), (i#0 > 3)] aggregates=[sum(cast(i#0 as bigint))], groupby=[j#1], havingClause==[sum(cast(i#0 as bigint))>10], options=Map())
   *
   * == Physical Plan ==
   * DataSourceV2Scan(source=AdvancedDataSourceV2,schema=[i#0 int, j#1 int],filters=[isnotnull(i#0), (i#0 > 3)] aggregates=[sum(cast(i#0 as bigint))], groupby=[j#1], havingClause==[sum(cast(i#0 as bigint))>10], options=Map())
   */
  @Test
  def testPushDownGroupBy(): Unit = {

    val conf = new SparkConf().set("spark.sql.sources.useV1SourceList", "")
      .set("spark.sql.catalog.mysql", classOf[JDBCTableCatalog].getName)
      .set("spark.sql.catalog.mysql.url", fullUrl)
      .set("spark.sql.catalog.mysql.driver", driver)
      .set("spark.sql.catalog.mysql.pushDownAggregate", "true")
      .set("spark.sql.catalog.mysql.pushDownLimit", "true")

    val spark = SparkSession.builder()
      .appName("MySQLTest")
      .master("local[2]")
      .config(conf)
      .getOrCreate()
    // one
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", user)
//    connectionProperties.put("password", password)
//    val df = spark.read.option("pushDownAggregate", "true").jdbc(url, "contact_meta_prop", connectionProperties)


    //    val tableName = "(select max(id),min(id) from contact_meta_prop) temp"

//        val df = spark.read
//          .format("jdbc")
//          .option("url", url)
//          .option("dbtable", "contact_meta_prop")
//          .option("user", user)
//          .option("password", password)
//          .option("numPartitions", "1")
////          .option("pushDownAggregate", "true")
//          //      .option("lowerBound", "0")
//          //      .option("upperBound", "100")
//          //      .option("partitionColumn", "id")
//          .load()

//    println(s"partitions num = ${df.rdd.getNumPartitions}")
//    df.createOrReplaceTempView("contact_meta_prop")
    val data = spark.sql("select max(id),min(id) from mysql.test.user where id > 10 group by name")
//        val data = spark.sql("select max(id),min(id) from mysql.test.user where id = 10")
//        val data = spark.sql("select id from mysql.test.user where id = 10")
    //    data.show()
    //    df.filter(col("tenant_id") === 1).groupBy(col("status")).agg(count(col("id"))).where(count(col("id")) > 10).explain(true)
    //    data.queryExecution.debug
    //    println(s"logical = ${spark.sessionState.analyzer.ResolveRelations(data.queryExecution.logical)}")
    println(data.explain("extended"))
  }


  @Test
  def testPushDownProjection(): Unit = {
    val spark = SparkSession.builder()
      .appName("testPushDownProjection")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val ds2 = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 11)).toDS()
    val ds3 = Seq(Person("Michael", 9), Person("Andy", 7), Person("Justin", 11)).toDS()

    // 进行数据集的union和select运算
    ds2.union(ds3).union(ds2).select("name").explain(true)
  }

  @Test
  def testPushDownWindow(): Unit = {
    val spark = SparkSession.builder()
      .appName("testPushDownProjection")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val empSalay = Seq(
      Salary("sales", 1, 5000),
      Salary("sales", 12, 3000),
      Salary("personnel", 2, 3000),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 6, 4200),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 5600),
      Salary("develop", 9, 6000),
      Salary("develop", 10, 7700),
      Salary("develop", 11, 8800)
    ).toDS()

    val byDepSalaryDesc = Window.partitionBy('dep).orderBy('salary desc)
    val rankByDep = rank().over(byDepSalaryDesc)
    //    val rankByDep = dense_rank().over(byDepSalaryDesc)
    //    val rankByDep = row_number().over(byDepSalaryDesc)
    empSalay.select('*, rankByDep as 'rank).where(col("dep") === ("develop")).explain(true)
    //    empSalay.select('*,rankByDep as 'rank).where(col("dep") === ("develop")).show()

  }

  /**
   * limit这种就不能下推，因为下推了再过滤就不对了，例如ds2
   */
  @Test
  def testPushDownLimit(): Unit = {
    val spark = SparkSession.builder()
      .appName("testPushDownProjection")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val ds1 = spark.createDataset(1 to 100).limit(10).filter('value % 10 === 0)
    val ds2 = spark.createDataset(1 to 100).filter('value % 10 === 0).limit(10)
    ds1.collect().foreach(println)
    ds1.explain(true)
    ds2.collect().foreach(println)
    ds2.explain(true)
  }

  @Test
  def testPushDownGroupBy1(): Unit = {
    val spark = SparkSession.builder()
      .appName("MySQLTest")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val empSalay = Seq(
      Salary("sales", 1, 5000),
      Salary("sales", 12, 3000),
      Salary("personnel", 2, 3000),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 6, 4200),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 5600),
      Salary("develop", 9, 6000),
      Salary("develop", 10, 7700),
      Salary("develop", 11, 8800)
    ).toDS()

    val df = empSalay.filter(col("dep") === "develop").groupBy(col("dep")).agg(functions.max("salary")).where(functions.max("salary") > 8000)
    df.explain(true)
  }


  @Test
  def testPushDownGroupBy2(): Unit = {
    val conf = new SparkConf().set(SQLConf.PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key, "true")
      .set("spark.sql.sources.useV1SourceList", "")

    val spark = SparkSession.builder()
      .appName("MySQLTest")
      .master("local[2]")
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    //    Seq((10, 1, 2, 5, 6), (2, 1, 2, 5, 6), (3, 2, 1, 4, 8), (4, 2, 1, 4, 9),
    //      (5, 2, 1, 5, 8), (6, 2, 1, 4, 8), (1, 1, 2, 5, 6), (4, 1, 2, 5, 6),
    //      (3, 2, 2, 9, 10), (-4, 2, 2, 9, 10), (6, 2, 2, 9, 10))
    //      .toDF("value", "p1", "p2", "p3", "p4")
    //      .write
    //      .partitionBy("p2", "p1", "p4", "p3")
    //      .format("parquet")
    //      .save("hdfs://localhost:9000/parquet3")
    spark.read.format("parquet").load("hdfs://localhost:9000/parquet2").createOrReplaceTempView("tmp")
    val query = "SELECT count(*), count(value), max(value), min(value)," +
      " p4, p2, p3, p1 FROM tmp GROUP BY p1, p2, p3, p4"
    val ds = spark.sql(query)
    ds.show()
    ds.explain(true)
  }
}
