package com.kopochaeproject

object Model_Seasonality {
  def main(args: Array[String]): Unit = {
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Library Definition ////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    import org.apache.spark.sql.SparkSession
    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Function Definition ////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // Function: Return movingAverage result
    // Input:
    //   1. Array : targetData: inputsource
    //   2. Int   : myorder: section
    // output:
    //   1. Array : result of moving average
    def movingAverage(targetData: Array[Double], myorder: Int): Array[Double] = {
      val length = targetData.size
      if (myorder > length || myorder <= 2) {
        throw new IllegalArgumentException
      } else {
        var maResult = targetData.sliding(myorder).map(_.sum).map(_ / myorder)

        if (myorder % 2 == 0) {
          maResult = maResult.sliding(2).map(_.sum).map(_ / 2)
        }
        maResult.toArray
      }
    }

    ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
    var spark = SparkSession.builder().config("spark.master","local").getOrCreate()

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Loading   ////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // Path setting
    //    var dataPath = "./data/"
    //    var mainFile = "kopo_channel_seasonality_ex.csv"
    //    var subFile = "kopo_product_master.csv"
    //
    //    var path = "c:/spark/bin/data/"
    //
    //    // Absolute Path
    //    //kopo_channel_seasonality_input
    //    var mainData = spark.read.format("csv").option("header", "true").load(path + mainFile)
    //    var subData = spark.read.format("csv").option("header", "true").load(path + subFile)
    //
    //    spark.catalog.dropTempView("maindata")
    //    spark.catalog.dropTempView("subdata")
    //    mainData.createTempView("maindata")
    //    subData.createOrReplaceTempView("subdata")
    //
    //    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //    ////////////////////////////////////  Data Refining using sql////////////////////////////////////////
    //    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //    var joinData = spark.sql("select a.regionid as accountid," +
    //      "a.product as product, a.yearweek, a.qty, b.productname " +
    //      "from maindata a left outer join subdata b " +
    //      "on a.productgroup = b.productid")
    //
    //    joinData.createOrReplaceTempView("keydata")
    // 1. data loading
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var staticUrl = "jdbc:oracle:thin:@192.168.0.10:1521/XE"
    staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("keydata")

    println(selloutDataFromOracle.show())
    println("oracle ok")

  }

}
