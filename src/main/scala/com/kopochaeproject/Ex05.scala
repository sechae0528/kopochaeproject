package com.kopochaeproject

import org.apache.spark.sql.SparkSession;

object Ex05 {
  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()

  // 접속정보 설정
  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb1 = "kopo_channel_seasonality_new"
  var selloutDb2 = "kopo_region_mst"

  // jdbc (java database connectivity) 연결
  val mainData1 = spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load
  val subData1= spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> selloutDb2,"user" -> staticUser, "password" -> staticPw)).load

  // 메모리 테이블 생성
  mainData1.createOrReplaceTempView("maindata1")
  subData1.createOrReplaceTempView("subdata1")
  mainData1.show()
  subData1.show()

  var innerJoinData = spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
    "from maindata1 a inner join subdata1 b " +
    "on a.regionid = b.regionid")


  var leftJoinData = spark.sql("select a.regionid, a.product, a.yearweek, cast(a.qty as Double), cast(b.regionname as String) " +
    "from maindata1 a left join subdata1 b " +
    "on a.regionid = b.regionid")

  // 파일저장
  leftJoinData.
    coalesce(1). // 파일개수
    write.format("csv").  // 저장포맷
    mode("overwrite"). // 저장모드 append/overwrite
    option("header", "true"). // 헤더 유/무
    save("c:/test_leftJoinData.csv") // 저장파일명


  // 데이터베이스 주소 및 접속정보 설정
  var outputUrl = "jdbc:oracle:thin:@192.168.110.5:1522/XE"
  //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
   var outputUser = "sechae"
    var outputPw = "sechae"
    // 데이터 저장
    var prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    var table = "test_leftJoinData"
    //append
    leftJoinData.write.mode("overwrite").jdbc(outputUrl, table, prop)


//한글 깨지지않게 rdd로 저장방법
  leftJoinData.rdd.coalesce(1).map { x =>x.mkString(",") }.saveAsTextFile("e:/test/test3.csv")


}
