package com.kopochaeproject

object Example_GroupBy2 {
  def main(args: Array[String]): Unit = {
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

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining
    //////////////////////////////////////////////////////////////////////////////////////////////////

    //    var mainDataSelectSql = "select regionid, regionname, ap1id, ap1name, accountid, accountname," +
    //      "salesid, salesname, productgroup, product, item," +
    //      "yearweek, year, week, " +
    //      "cast(qty as double) as qty," +
    //      "cast(target as double) as target," +
    //      "idx from selloutTable where 1=1"
    var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as String) as qty, " +
      "'test' as productname from keydata a" )

    rawData.show(2)

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    var rawRdd = rawData.rdd

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // The abnormal value is refined using the normal information
    var filterRdd = rawRdd.filter(x=>{

      // Data comes in line by line
      var checkValid = true
      // Assign yearweek information to variables
      var week = x.getString(yearweekNo).substring(4,6).toInt
      // Assign abnormal to variables
      var standardWeek = 52

      // filtering
      if (week > standardWeek)
      {
        checkValid = false
      }
      checkValid
    })

    // key, account, product, yearweek, qty, productname
    var mapRdd = filterRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var maxValue = 700000
      if(qty > 700000){qty = 700000}
      Row( x.getString(keyNo),
        x.getString(accountidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        qty, //x.getString(qtyNo),
        x.getString(productnameNo))
    })

    var groupRdd = mapRdd.groupBy(x=>{
      (x.getString(keyNo))
    }).map(x=>{

      // key 값
      var key = x._1
      //기본 데이터
      var data = x._2
      var dataSize = data.size

      var sumation = data.map(x=>{x.getDouble(3)}).sum
      var average = 0.0
      if(dataSize == 0){
        average = 0.0d
      }else {
        average = sumation/dataSize
      }
      (key, sumation, average)
      // (key, data)
      // (key, dataSize)
    })

    var groupRdd2 = mapRdd.groupBy(x=>{
      (x.getString(keyNo))
    }).flatMap(x=>{

      // key 값
      var key = x._1
      //기본 데이터
      var data = x._2
      var dataSize = data.size

      var sumation = data.map(x=>{x.getDouble(3)}).sum
      var average = 0.0
      if(dataSize == 0){
        average = 0.0d
      }else {
        average = sumation/dataSize
      }

      var outputData = data.map(x=> {

        Row(x.getString(0), //key 정보 (거래처, 상품)
          x.getString(1), // 주차 정보 (yearweek)
          x.getDouble(2), //original Qty
          x.getDouble(3), //revision Qty
          average) // key(거래처, 상품)의 평균
      })
      outputData
    })




  }
}
