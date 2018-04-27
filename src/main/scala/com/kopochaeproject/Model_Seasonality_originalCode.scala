package com.kopochaeproject

import com.kopo
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.descriptive.SummaryStatistics
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

object Model_Seasonality_originalCode {
  // function: movingAverage
  // input parameter:
  //   1. targetData: sequence data of qty (ex: default 1~52)
  //	  2. myorder: section of movingaverage (ex: default 9)
  def movingAverage(targetData: Iterable[Double], myorder: Int): List[Double] = {
    val length = targetData.size
    if (myorder > length || myorder <= 2) {
      throw new IllegalArgumentException
    } else {
      var maResult = targetData.sliding(myorder).map(_.sum).map(_ / myorder)

      if (myorder % 2 == 0) {
        maResult = maResult.sliding(2).map(_.sum).map(_ / 2)
      }
      maResult.toList
    }
  }

  //getSeasonality(splitData, myorder, myIter, qtyColumnNum)
  // var myproduct = splitData
  // var useQtyColumnNum = qtyColumnNum
  def getSeasonality(myproduct: Iterable[(Row, String)], myorder: Int, myIter: Int,
                     useQtyColumnNum: Int, yearweekColumnNum: Int): Iterable[((Row, String), Double, Double)] = {

    // suborder is half of myorder (ex 17 -> 8)
    val subOrder = (myorder.toDouble / 2.0).floor.toInt

    //mysave2 = mysave.toSeq.sortBy(x=>{(x._1.getString(1),x._1.getString(3))})
    var rowQtySeqData = myproduct.toSeq.sortBy(x=>{(x._1.getString(3))}).map(x => { x._1.get(useQtyColumnNum).toString.toDouble  }).zipWithIndex
    //var rowQtySeqData = myproduct.map(x => { x._1.get(useQtyColumnNum).toString.toDouble  }).zipWithIndex
    val countData = myproduct.size
    var finalOTMA = new ArrayBuffer[Double]
    var finalSD = new ArrayBuffer[Double]

    // change abnormal qty value to normal qty value
    for (iter <- 0 until myIter) {

      // get qty applied movingAverageLogic
      var OTMA = movingAverage(rowQtySeqData.map(x => x._1), myorder)

      // Setting preValue that moving average cannot cover
      val preMAArray = new ArrayBuffer[Double]
      // Setting postValue that moving average cannot cover
      var postMAArray = new ArrayBuffer[Double]

      // Calculate pre & post Value using average
      for (j <- 0 until subOrder) {
        preMAArray.append(rowQtySeqData.filter(x => x._2 >= 0 && x._2 <= (j + subOrder)).map(x =>
          x._1).sum / (j + subOrder + 1).toDouble)

        postMAArray.append(rowQtySeqData.filter(x => x._2 >= (countData.toInt - j - (subOrder)
          - 1) && x._2 < (countData)).map(x => x._1).sum / (countData.toInt - (countData.toInt - j -
          (subOrder) - 1)).toDouble)
      }

      // Union the result of MovingAverage (OTMA) with pre, post result
      postMAArray = postMAArray.reverse
      finalOTMA = preMAArray.union(OTMA)
      finalOTMA = finalOTMA.union(postMAArray)

      rowQtySeqData = finalOTMA.zipWithIndex

    }

    val seasonality = myproduct.toSeq.sortBy(x=>{x._1.getString(yearweekColumnNum)}).
      zip(rowQtySeqData).map(data => {
      val rawSeasonality = data._1._1.get(useQtyColumnNum).toString.toDouble / data._2._1 - 1.0
      (data._1, data._2._1, rawSeasonality)
    })
    seasonality.toIterable
  }
  var spark = SparkSession.builder().config("spark.master","local").getOrCreate()

  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality2"

  // jdbc (java database connectivity) 연결
  val selloutDataFromOracle = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

  // 메모리 테이블 생성
  selloutDataFromOracle.registerTempTable("selloutTable")

  var maindf = spark.sql("select regionid, productgroup, product, yearweek, year, week, cast(qty as Double) as qty from selloutTable")


  val maindfcol = maindf.columns.map(x=>{x.toLowerCase()})
  val regionidColumnNum = maindfcol.indexOf("regionid")
  val productgColumnNum = maindfcol.indexOf("productgroup")
  val productColumnNum = maindfcol.indexOf("product")
  val yearweekColumnNum = maindfcol.indexOf("yearweek")
  val yearColumnNum = maindfcol.indexOf("year")
  val weekColumnNum = maindfcol.indexOf("week")
  val qtyColumnNum = maindfcol.indexOf("qty")

  val myorder = 9//season_parameters.rdd.filter(row =>
  val myIter = 4//season_parameters.rdd.filter(row =>
  val resultTableName = "sesaon_result"//prefix + "_" + jobid + "_season_result"

  var resultUnion = maindf.rdd.filter(row => {
    var checkValid = true

    checkValid && !row.getString(weekColumnNum).equalsIgnoreCase("53")
  }).groupBy { x => (x.getString(regionidColumnNum), x.getString(productgColumnNum))
  }.flatMap(splitRow => {
    /////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////logic start //////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////
    // var splitRow = resultUnion.first
    val inputMyData = splitRow._2

    // 1. Get list of product in productgroup
    val distinctProduct = inputMyData.map { x => x.getString(productColumnNum) }.toSeq.distinct
    val productInfo = new ArrayBuffer[(String, String)]

    // 2. flag check
    for (i <- 0 until distinctProduct.length) {
      val currProductName = distinctProduct(i)

      val flagCheck = inputMyData.filter { x =>
        x.getString(productColumnNum).equalsIgnoreCase(currProductName) && x.get(qtyColumnNum).toString.toDouble < 10
      }
      if (flagCheck.size > 0) {
        productInfo.append((distinctProduct(i), "G"))
      } else {
        productInfo.append((distinctProduct(i), "P"))
      }
    }
    // define Variables
    var pp = 0
    var gg = 0

    var myResult: Iterable[((Row, String), Double, Double)] = Iterable.empty
    var mysave: Iterable[(Row, String)] = Iterable.empty
    var mysave2: Iterable[(Row, String)] = Iterable.empty

    for (i <- 0 until distinctProduct.length) {
      val currProductName = distinctProduct(i)
      val currProductInfo = productInfo(i)

      // splitData equal past week's only qty
      val splitData = inputMyData.filter { x =>
        x.getString(productColumnNum).equalsIgnoreCase(currProductName) }.map { x => (x,
        currProductInfo._2) }

      // var currProductInfo._2 = "P"
      // get Seasonality of 'P' Product'
      if (currProductInfo._2.equalsIgnoreCase("P"))
      {
        pp = 1
        mysave = mysave ++ splitData
        myResult = myResult ++ getSeasonality(splitData, myorder, myIter, qtyColumnNum, yearweekColumnNum)
      }else {
        gg = 1
      }
    }

    var mResult = myResult.groupBy{x=>(x._1._1.getString(productColumnNum),x._1._1.getString(weekColumnNum))}.map(x=>
    {
      var finalseasonality = 0.0d
      var movingAverageValue = 0.0d
      var realQty = 0.0d

      var data = x._2

      var seasonData = data.map(x=>{ (x._1._1.get(qtyColumnNum).toString, x._2, x._3 )})

      realQty = seasonData.map(x=>{x._1.toDouble}).sum / seasonData.size
      movingAverageValue = seasonData.map(x=>{x._2}).sum / seasonData.size
      finalseasonality = seasonData.map(x=>{x._3}).sum / seasonData.size

      (x._1._1, x._1._2 , realQty, movingAverageValue, finalseasonality)

    })

    var addWeek: Iterable[(String, String, Double, Double, Double)] = Iterable.empty
    for (i <- 0 until distinctProduct.length) {
      val currProductName = distinctProduct(i)

      val currWeek1 = mResult.filter(data => { data._1.equalsIgnoreCase(currProductName) &&
        (data._2.equalsIgnoreCase("1") || data._2.equalsIgnoreCase("01")) })

      val currWeek52 = mResult.filter(data => { data._1.equalsIgnoreCase(currProductName) &&
        data._2.equalsIgnoreCase("52") })

      val week53Seasonality = (currWeek1.head._5 + currWeek52.head._5) / 2.0
      val week53Data = currWeek1.take(1).map(x=>{ (x._1, "53", x._3, x._4, week53Seasonality)})
      addWeek = week53Data ++ addWeek
    }

    var finalSeasonality = myResult.map(x=>{
      (x._1._1.getString(regionidColumnNum),
        x._1._1.getString(productgColumnNum),
        x._1._1.getString(productColumnNum),
        x._1._1.getString(yearweekColumnNum),
        x._1._1.getString(yearColumnNum),
        x._1._1.getString(weekColumnNum),
        x._1._1.get(qtyColumnNum).toString,
        Math.round(x._2).toString,
        x._3.toString)
    })
    var final_result = finalSeasonality.
      map(x=>{ Row.fromSeq(Seq(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9))})
    final_result

  })

  val finalResultDF = spark.createDataFrame(resultUnion, StructType(Seq(StructField("REGIONID",
    StringType), StructField("PRODUCTGROUP", StringType), StructField("PRODUCT", StringType),
    StructField("YEARWEEK", StringType), StructField("YEAR",
      StringType), StructField("WEEK", StringType), StructField("QTY", StringType),
    StructField("MA", StringType), StructField("SEASON", StringType))))

  finalResultDF.
    coalesce(1). // 파일개수
    write.format("csv"). // 저장포맷
    mode("overwrite"). // 저장모드 append/overwrite
    option("header", "true"). // 헤더 유/무
    save("seasonFinalResult.csv") // 저장파일명




}
