package com.hjy.dataprocess



import java.util
import java.util.stream.Collectors

import com.hjy.uilts.Uilts._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Durability, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization

import scala.util.parsing.json.JSONObject

object DataProcessing {

  val hbaseQuorum = "hadoop10,hadoop11,hadoop12,hadoop13"
  val zookeeperPort = "2181"
  val tableName = "tmdb"
  val tmdb_popularity = "tmdb_popularity"
  val tmdb_runtime = "tmdb_runtime"


  def main(args: Array[String]): Unit = {

    //spark配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DataProcessing")
    //创建spark会话
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
//    val sqlContext = new SQLContext(sc)

    import spark.implicits._
//    import sqlContext.implicits._


    //hbase配置
    val hbaseConf = getHbaseConf(hbaseQuorum, zookeeperPort)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hbaseRDD = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    println(hbaseRDD.count())

    //把数据导出成df格式
    val df = hbaseRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("budget"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("genres"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("homepage"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("id"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("keywords"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("original_language"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("original_title"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("overview"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("popularity"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("production_companies"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("production_countries"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("release_date"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("revenue"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("runtime"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("spoken_languages"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("status"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("tagline"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("title"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("vote_average"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("vote_count")))
    )).toDF("budget","genres","homepage","id","keywords","original_language","original_title",
        "overview","popularity","production_companies","production_countries","release_date",
        "revenue","runtime", "spoken_languages","status", "tagline","title","vote_average","vote_count")


    df.show()
    println(df.getClass)

    //体裁分布
//    val genres = df.select("genres").rdd.foreach(x=>println(x(0)))
//    def getJson(): Unit ={
//      import org.json4s.jackson.Serialization.write
//      implicit val formats:AnyRef with Formats = Serialization.formats(NoTypeHints)
//      val genres = df.select("genres").filter(_ != "").rdd.foreach(x=>println(write(x(0))))
//    }
//    getJson()


    //前100个创建关键词


    //流行度
    def getPopularity(df:DataFrame): Unit = {
      var popularity = df.select("original_title", "popularity")
      popularity = popularity.withColumn("popularity", 'popularity.cast("Float"))
//      popularity.show(20, false)
      popularity = popularity.orderBy(popularity("popularity").desc)
      //    popularity = popularity.sort(popularity("popularity").desc)

      //随机唯一索引
//      import org.apache.spark.sql.functions._
//      popularity = popularity.withColumn("id", monotonically_increasing_id)
//      popularity = popularity.filter(popularity("id") < 10)

      //建立自增索引
      // 在原Schema信息的基础上添加一列 “id”信息
      val schema: StructType = popularity.schema.add(StructField("id", LongType))
      // DataFrame转RDD 然后调用 zipWithIndex
      val dfRDD: RDD[(Row, Long)] = popularity.rdd.zipWithIndex()
      val rowRDD: RDD[Row] = dfRDD.map(tp => Row.merge(tp._1, Row(tp._2)))
      // 将添加了索引的RDD 转化为DataFrame
      var popularitydf = spark.createDataFrame(rowRDD, schema)
      popularitydf = popularitydf.filter("id<10")
      popularitydf.show(20, false)

      //转换成rdd
      val rdd = popularitydf.rdd

      val jobConf : JobConf = new JobConf(hbaseConf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, tmdb_popularity)
      try {
        rdd.map(t=>{
          val put = new Put(Bytes.toBytes(t.getLong(2)))
          put.addColumn("info".getBytes, "id".getBytes, Bytes.toBytes(t.getLong(2)))
          put.addColumn("info".getBytes, "original_title".getBytes, Bytes.toBytes(t.getString(0)))
          put.addColumn("info".getBytes, "popularity".getBytes, Bytes.toBytes(t.getFloat(1)))
          put.setDurability(Durability.ASYNC_WAL)
          (new ImmutableBytesWritable(),put)
        }).saveAsHadoopDataset(jobConf)
        println("=============popularity数据批量插入成功=============")
      } catch {
        case e:Exception =>println("popularity数据批量插入失败："+e)
      }
    }
    getPopularity(df)

    //统计电影时长
    def getRuntime(df:DataFrame):Unit={
      //建立随机唯一索引
//      import org.apache.spark.sql.functions._
//      val runtime = df.filter("runtime!=0").groupBy("runtime").count().filter("count>=100").withColumn("id", monotonically_increasing_id)

      //建立自增索引
      val runtime = df.filter("runtime!=0").groupBy("runtime").count().filter("count>=100")
      // 在原Schema信息的基础上添加一列 “id”信息
      val schema: StructType = runtime.schema.add(StructField("id", LongType))
      // DataFrame转RDD 然后调用 zipWithIndex
      val dfRDD: RDD[(Row, Long)] = runtime.rdd.zipWithIndex()
      val rowRDD: RDD[Row] = dfRDD.map(tp => Row.merge(tp._1, Row(tp._2)))
      // 将添加了索引的RDD 转化为DataFrame
      val runtimedf = spark.createDataFrame(rowRDD, schema)
      runtimedf.show()

      //转换成rdd
      val rdd  = runtimedf.rdd

      val jobConf : JobConf = new JobConf(hbaseConf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, tmdb_runtime)
      try {
        rdd.map(t=>{
          val put = new Put(Bytes.toBytes(t.getLong(2)))
          put.addColumn("info".getBytes, "id".getBytes, Bytes.toBytes(t.getLong(2)))
          put.addColumn("info".getBytes, "runtime".getBytes, Bytes.toBytes(t.getString(0)))
          put.addColumn("info".getBytes, "count".getBytes, Bytes.toBytes(t.getLong(1)))
          put.setDurability(Durability.ASYNC_WAL)
          (new ImmutableBytesWritable(),put)
        }).saveAsHadoopDataset(jobConf)
        println("=================runtime数据批量插入完成================")
      } catch {
        case e:Exception =>println("runtime数据批量插入失败："+e)
      }
    }
    getRuntime(df)


  }
}
