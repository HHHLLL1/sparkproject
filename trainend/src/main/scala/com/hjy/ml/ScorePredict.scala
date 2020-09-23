package com.hjy.ml

import com.hjy.dataprocess.DataProcessing.{hbaseQuorum, tableName, tmdb_popularity, zookeeperPort}
import com.hjy.uilts.Uilts.getHbaseConf
import org.apache.hadoop.hbase.client.{Durability, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}


object ScorePredict {

  val hbaseQuorum = "hadoop10"
  val zookeeperPort = "2181"
  val tableName = "tmdb"
  val tmdb_voteave = "tmdb_voteave"


  def main(args: Array[String]): Unit = {

    //spark配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ScorePredict")
    //创建spark会话
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", true)
    val sc = spark.sparkContext
    //    val sqlContext = new SQLContext(sc)

    import spark.implicits._


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
    var df = hbaseRDD.map(r=>(
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

//    df.show()

    df = df.select("id", "budget", "popularity", "revenue", "runtime", "vote_count", "vote_average")
    df = df.withColumn("budget", 'budget.cast("Float"))
    df = df.withColumn("popularity", 'popularity.cast("Float"))
    df = df.withColumn("revenue", 'revenue.cast("Float"))
    df = df.withColumn("runtime", 'runtime.cast("Float"))
    df = df.withColumn("vote_count", 'vote_count.cast("Float"))
    df = df.withColumn("vote_average", 'vote_average.cast("Float"))
//    df.show()

    var train = df.filter("vote_average!=0")
    var test = df.filter("vote_average==0")
    train.show()
    test.show()

    val colArray = Array("budget", "popularity", "revenue", "runtime", "vote_count", "vote_average")
    val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
    val x_train = assembler.transform(train.na.drop)
    var lr = new LinearRegression()
    lr = lr
      .setFeaturesCol("features")
      .setLabelCol("vote_average")
      .setFitIntercept(true)
      .setMaxIter(20)
      .setRegParam(0.5)
      .setElasticNetParam(0.5)

    val model = lr.fit(x_train)
    model.extractParamMap()

    // 模型进行评价
    val trainingSummary = model.summary
    trainingSummary.residuals.show()
    println(s"均方根差: ${trainingSummary.rootMeanSquaredError}")//RMSE:均方根差
    println(s"判定系数: ${trainingSummary.r2}")//r2:判定系数，也称为拟合优度，越接近1越好


    val colArray_for_predict = Array("budget", "popularity", "revenue", "runtime", "vote_count", "vote_average")
    val assembler_pre = new VectorAssembler().setInputCols(colArray_for_predict).setOutputCol("features")
    val x_test = assembler_pre.transform(test.na.drop)

    val predictions = model.transform(x_test)
    var predict_result = predictions.selectExpr("features", "vote_average", "round(prediction,1) as prediction")
    predict_result = predict_result.select("prediction")
    predict_result.show()


    val df2 = train.select("vote_average")
    var df1 = df2.union(predict_result)
    df1.show()
    println()
    val id_ = train.select("id").union(test.select("id"))
    id_.show()
    val new_df = id_.join(df1).select("id", "vote_average")
//    val new_df = id_.withColumn("vote_average", df1("vote_average"))
//    val new_df = df.select("id").join(df1)
    print(new_df.show())

    def getVoteAverage(df:DataFrame): Unit = {
      var vote_average = df.withColumn("id", 'id.cast("Int"))
      vote_average = vote_average.orderBy(vote_average("id"))

      //转换成rdd
      val rdd = vote_average.rdd

      val jobConf : JobConf = new JobConf(hbaseConf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, tmdb_voteave)
      try {
        rdd.map(t=>{
          val put = new Put(Bytes.toBytes(t.getInt(0)))
          put.addColumn("info".getBytes, "id".getBytes, Bytes.toBytes(t.getInt(0)))
          put.addColumn("info".getBytes, "vote_average".getBytes, Bytes.toBytes(t.getDouble(1)))
          put.setDurability(Durability.ASYNC_WAL)
          (new ImmutableBytesWritable(),put)
        }).saveAsHadoopDataset(jobConf)
        println("=============vote_average数据批量插入成功=============")
      } catch {
        case e:Exception =>println("vote_average数据批量插入失败："+e)
      }
    }
    getVoteAverage(new_df)

    sc.stop()

  }

}
