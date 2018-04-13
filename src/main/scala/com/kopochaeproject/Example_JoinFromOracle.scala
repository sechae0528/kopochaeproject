package com.kopochaeproject
import org.apache.spark.sql.SparkSession;


object Example_JoinFromOracle {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var masterDb = "kopo_product_master"

    val selloutDf = spark.read.format("jdbc").option("encoding", "UTF-8").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    val mstDf = spark.read.format("jdbc").option("encoding", "UTF-8").
      options(Map("url" -> staticUrl, "dbtable" -> masterDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")

    mstDf.createOrReplaceTempView("mstTable")

    var resultDf = spark.sql("select " +
      "concat(a.regionid, concat('_',a.product)) AS KEYCOL, "+
      "a.regionid AS REGIONID, " +
      "a.product AS PRODUCT, " +
      "a.yearweek AS YEARWEEK, " +
      "cast(qty as double) AS QTY, " +
      "b.productname AS PRODUCTNAME " +
      "from selloutTable A " +
      "left join mstTable B " +
      "on a.product = b.productid")

    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) AS KEYCOL, "+
      "a.regionid AS ACCOUNTID, " +
      "a.product AS PRODUCT, " +
      "a.yearweek AS YEARWEEK, " +
      "cast(qty as double) AS QTY, " +
      "b.productname AS PRODUCTNAME " +
      "from selloutTable A " +
      "left join mstTable B " +
      "on a.product = b.productid")


    //컬럼에 인덱스 생성

    var rawDataColumns = rawData.columns

    var keyNo = rawDataColumns.indexOf("KEYCOL")
    var accountidNo = rawDataColumns.indexOf("ACCOUNTID")
    var productNo = rawDataColumns.indexOf("PRODUCT")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var qtyNo = rawDataColumns.indexOf("QTY")
    var productnameNo = rawDataColumns.indexOf("PRODUCTNAME")


   // 컬럼 배열확인방법 (QTY가 4번째에 있는 것을 확인)
    rawDataColumns.indexOf("QTY")

    //컬럼명이 들어있는 지 확인할 때, 먼저 Set으로 만들어준다.
    var dataSet = rawDataColumns.toSet
    //다음 contains으로 컬럼명에 QTY가 있는지 확인.
    rawDataColumns.contains("QTY")

    //컬럼명 바꿀 때,
    var testDf = rawData.toDF("AA","BB","CC","DD","EE","FF")
    //컬럼명이 바뀌고 생성된 데이터를 볼 수 있음.
    testDf.show(1)

    //대소문자상관없이 같은지 확인하고 싶을 때,
    //첫번째 방법
    var test1 = "aabbCCDD"
    var test2 = "AABBCCdd"
    test1.equalsIgnoreCase(test2)
    //두번째 방법 (둘다 소문자로 바꾸고 확인하는 방법)
    var a = test1.toLowerCase()
    var b = test2.toLowerCase()
    a== b



    var myUrl = "jdbc:oracle:thin:@127.0.0.1:1522/XE"

    // 데이터 저장
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", "haiteam")
    prop.setProperty("password", "haiteam")
    val table = "hk_join"
    //append
    resultDf.write.mode("overwrite").jdbc(myUrl, table, prop)

    // Dataframe.toDf.....
    ///////////// Oracle Express Usage ////////////////
    //alter system set processes=500 scope=spfile
    //show parameter processes
    //shutdown immediate
    //startup


    rawData.
      coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("charset", "ISO-8859-1").
      option("header", "true"). // 헤더 유/무
      save("e:/resultdf2.csv")
    //ISO-8859-1 ISO-8859-1

    rawData.rdd.coalesce(1).map { x =>x.mkString(",") }.saveAsTextFile("e:/test/test3.csv")

    //RDD 변환
    var rawRdd = rawData.rdd

    //RDD 처리(데이터 정제)
    //(KECOL, ACCOUNTID, PRODUCT, YEARWEEK, QTY, PRODUCTNAME)
    var filterexRdd = rawRdd.filter(x=> {
      // 데이터 한줄씩 들어옴
      var checkValid = true
      // 그중 특정 컬럼값
      var yearweek = x.getString(yearweekNo)

      if(yearweek.length != 6){
        checkValid = false
      }
      //if(x.getString(yearweekNo).length != 6) {checkValid = false}
      checkValid
    })


    //디버깅 방법 (case1)
    var filterexRdd1 = rawRdd
    var x = filterexRdd1.first


    //디버깅 방법 (case2)
    var filterex2Rdd = rawRdd.filter(x=>{
      (x.getString(yearweekNo) == "201512")
    })
    var x1 = filterex2Rdd.first

    var filterex3Rdd = rawRdd.filter(x=>{
      var checkValid = false
      if((x.getString(accountidNo) =="A60") &&
        (x.getString(productNo) == "PRODUCT34") &&
        (x.getString(yearweekNo) == "201402")){
        checkValid = true
      }
      checkValid
    })
    var x2 = filterex3Rdd.first

    var filterex4Rdd = rawRdd.filter(x2=>{
      var checkValid = true
      if (x2.getString(yearweekNo).substring(4,2) > "52" ){
        checkValid = false
      }
      checkValid
    })

s
    //필터된 데이터 갯수를 확인하고 싶을 때,
    filterexRdd.count()




  }
}
