package com.kopochaeproject
import org.apache.spark
import org.apache.spark.sql.SparkSession

object MiddleTest {


  /////////////////////////////1번문제/////////////////////////////////////////

  // 접속정보 설정
  var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"

  // jdbc (java database connectivity) 연결
  val selloutData= spark.read.format("jdbc").option("encoding", "UTF-8").options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

  //1번답
  selloutData.show(2)

  /////////////////////////////2번문제/////////////////////////////////////////

  //2번답
  selloutData.createOrReplaceTempView("selloutTable")
  var rawData = spark.sql("select " +
    "regionid AS REGIONID, " +
    "product AS PRODUCT, " +
    "yearweek AS YEARWEEK, " +
    "cast(qty as double) AS QTY, " +
    "cast(qty * 1.2 as double) as QTY_NEW " +
    "from selloutTable")

  /////////////////////////////4번문제/////////////////////////////////////////

  //컬럼에 인덱스 생성
  var rawDataColumns = rawData.columns

  var regionidNo = rawDataColumns.indexOf("REGIONID")
  var productNo = rawDataColumns.indexOf("PRODUCT")
  var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
  var qtyNo = rawDataColumns.indexOf("QTY")
  var qtynewNo = rawDataColumns.indexOf("QTY_NEW")

  //RDD로 변환
  var rawRdd = rawData.rdd

  //4번답
  //RDD로 변환 후 정제하기
  var filteredRdd = rawRdd.filter(x => {

    var checkValid = false

    var yearValue = x.getString(yearweekNo).substring(0,4).toInt
    var weekValue = x.getString(yearweekNo).substring(4).toInt
    var productArray = Array("PRODUCT1", "PRODUCT2")
    var productSet = productArray.toSet
    var productInfo = x.getString(productNo)

    if (yearValue >= 2016 && weekValue != 52 && productSet.contains(productInfo)) {
      checkValid = true
    }
    checkValid
  })

  //필터링한 것 확인
  //모두 확인할 때,
  filteredRdd.collect.toArray.foreach(println)
  //3개만 확인할 때,
  filteredRdd.take(3).toArray.foreach(println)

  /////////////////////////////5번문제/////////////////////////////////////////

  //Rdd에서 Dataframe으로 변환시 타입 import 해주기!!
  import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}

  // 데이터형 변환 [RDD → Dataframe]
  val finalResultDf = spark.createDataFrame(filteredRdd ,
    StructType(
      Seq(
        StructField("regionid", StringType),
        StructField("product", StringType),
        StructField("yearweek", StringType),
        StructField("qty", DoubleType),
        StructField("qty_new", DoubleType))))

  //데이터 저장
  var myUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"

  val prop = new java.util.Properties
  prop.setProperty("driver", "org.postgresql.Driver")
  prop.setProperty("user", "kopo")
  prop.setProperty("password", "kopo")
  val table = "KOPO_ST_RESULT_CSE"
  //append
  finalResultDf.write.mode("overwrite").jdbc(myUrl  , table, prop)



}
