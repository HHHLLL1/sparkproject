package com.hjy.dataloader

import com.hjy.uilts.Uilts._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Durability, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DataToHbase {
  val hbaseQuorum = "hadoop10,hadoop11,hadoop12,hadoop13"
  val zookeeperPort = "2181"
  val tableName = "tmdb"


  // 小数据批量插入或更新hbase
  def insertSmallData(hbaseConf:Configuration,
                      tableName:String,
                      rdd:RDD[(String,String,String,String,String,String,String,String,String,String,String,
                                String,String, String, String,String, String,String, String, String,String)]): Unit ={
    val jobConf : JobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    try {
      rdd.map(t=>{
        val put = new Put(Bytes.toBytes(t._1))
        put.addColumn("info".getBytes, "budget".getBytes, t._2.getBytes)
        put.addColumn("info".getBytes, "genres".getBytes, t._3.getBytes)
        put.addColumn("info".getBytes, "homepage".getBytes, t._4.getBytes)
        put.addColumn("info".getBytes, "id".getBytes, t._5.getBytes)
        put.addColumn("info".getBytes, "keywords".getBytes, t._6.getBytes)
        put.addColumn("info".getBytes, "original_language".getBytes, t._7.getBytes)
        put.addColumn("info".getBytes, "original_title".getBytes, t._8.getBytes)
        put.addColumn("info".getBytes, "overview".getBytes, t._9.getBytes)
        put.addColumn("info".getBytes, "popularity".getBytes, t._10.getBytes)
        put.addColumn("info".getBytes, "production_companies".getBytes, t._11.getBytes)
        put.addColumn("info".getBytes, "production_countries".getBytes, t._12.getBytes)
        put.addColumn("info".getBytes, "release_date".getBytes, t._13.getBytes)
        put.addColumn("info".getBytes, "revenue".getBytes, t._14.getBytes)
        put.addColumn("info".getBytes, "runtime".getBytes, t._15.getBytes)
        put.addColumn("info".getBytes, "spoken_languages".getBytes, t._16.getBytes)
        put.addColumn("info".getBytes, "status".getBytes, t._17.getBytes)
        put.addColumn("info".getBytes, "tagline".getBytes, t._18.getBytes)
        put.addColumn("info".getBytes, "title".getBytes, t._19.getBytes)
        put.addColumn("info".getBytes, "vote_average".getBytes, t._20.getBytes)
        put.addColumn("info".getBytes, "vote_count".getBytes, t._21.getBytes)
        put.setDurability(Durability.ASYNC_WAL)
        (new ImmutableBytesWritable(),put)
      }).saveAsHadoopDataset(jobConf)
    } catch {
      case e:Exception =>println("数据批量插入失败："+e)
    }
  }


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DataToHbase")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val hbaseConf = getHbaseConf(hbaseQuorum, zookeeperPort)
    val table = getTable(hbaseConf, tableName)


    val txtpath = "E:\\java1\\sparkBigProject\\trainend\\src\\main\\resources\\tmdb.csv"
    val rdd = sc.textFile(txtpath).map(t=>{
      val arr = t.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1)
      (arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12),arr(13),arr(14),arr(15),arr(16),arr(17),arr(18),arr(19),arr(20))
    })
    insertSmallData(hbaseConf, tableName, rdd)

    println("=============Succeed=================")


  }

}
