package com.hjy.uilts

import java.net.URI

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2,LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Uilts {

  //获取hbase配置对象
  def getHbaseConf(hbaseQuorum:String, zookeeperPort:String): Configuration = {
    var hbaseConf: Configuration = null
    try {
      hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", hbaseQuorum)
      hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeperPort)
    }catch {
      case e:Exception => println("========Hbase连接失败:" + e)
    }
    return hbaseConf
  }

  //获取table对象
  def getTable(hbaseConf:Configuration, tableName:String): Table = {
    var table: Table = null
    try{
      val conn = ConnectionFactory.createConnection(hbaseConf)
      table = conn.getTable(TableName.valueOf(tableName))
    }catch {
      case e: Exception => println("========table对象获取失败:" + e)
    }
    return table
  }

//  // 小数据批量插入或更新hbase
//  def insertSmallData(hbaseConf:Configuration, tableName:String,rdd:RDD[(String, String, String)]): Unit ={
//    val jobConf : JobConf = new JobConf(hbaseConf)
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//    try {
//      rdd.map(t=>{
//        val put = new Put(Bytes.toBytes(t._1))
//        put.addColumn("info".getBytes, "name".getBytes, t._2.getBytes)
//        put.addColumn("info".getBytes, "age".getBytes, t._3.getBytes)
//        put.setDurability(Durability.ASYNC_WAL)
//        (new ImmutableBytesWritable(),put)
//      }).saveAsHadoopDataset(jobConf)
//      rdd.saveAsHadoopDataset(jobConf)
//    } catch {
//      case e:Exception =>println("小数据批量插入失败："+e)
//    }
//  }

  // 大数据批量插入或更新hbase
  def insertBigData(hbaseConf:Configuration, tableName:String,rdd:RDD[(ImmutableBytesWritable,KeyValue)]): Unit ={
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConf = new JobConf(hbaseConf)
    val load = new LoadIncrementalHFiles(hbaseConf)

    val hdfsFile="/testdata/tmp/bulkInsertHbase"
    val path = new Path(hdfsFile)
    val fileSystem = FileSystem.get(URI.create(hdfsFile), new Configuration())
    if(fileSystem.exists(path)){
      fileSystem.delete(new Path(hdfsFile),true)
    }

    rdd.saveAsNewAPIHadoopFile(hdfsFile,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],hbaseConf)
    //Thread.sleep(20000)
    load.doBulkLoad(new Path(hdfsFile),new HTable(hbaseConf,tableName))
  }


//  // 批量查询hbase(根据rowKey)
//  def scanData(table:Table ,startRowKey:String,stopRowKey:String)={
//    val scan = new Scan().withStartRow(Bytes.toBytes(startRowKey))
//      .withStopRow(Bytes.toBytes(stopRowKey))
//    val rs = table.getScanner(scan)
//    try {
//      val resultScan = rs.iterator()
//      while(resultScan.hasNext){
//        val result = resultScan.next().rawCells()
//        for(i <- 0.until(result.length)){
//          val family = Bytes.toString(CellUtil.cloneFamily(result(i)))
//          val rowKey = Bytes.toString(CellUtil.cloneRow(result(i)))
//          val column = Bytes.toString(CellUtil.cloneQualifier(result(i)))
//          val value = Bytes.toString(CellUtil.cloneValue(result(i)))
//          println(s"$family:$rowKey,$column:$value")
//        }
//      }
//    } catch {
//      case e:Exception =>println("批量查询操作失败："+e)
//    } finally {
//      rs.close()
//      table.close()
//    }
//  }

}
