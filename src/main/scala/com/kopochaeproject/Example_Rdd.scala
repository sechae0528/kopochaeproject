package com.kopochaeproject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
//Rdd에서 Dataframe으로 변환시 타입 import 해주기!!
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}

object Example_Rdd {

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


    /////////////////////////////////////RDD로 변환 ///////////////////////////
    var rawRdd = rawData.rdd


    //////////////////////////////RDD로 변환 후 정제하기(filter) ////////////////////////
    // 데이터형 변환 [데이터프레임 → RDD]
    //var {RDD변수명} = {RDD변수명}.filter(x=>{ 필터조건식})
    // 데이터 확인
    //var {RDD변수명}.collect.foreach(println)

    //필터예제 : 자신이 생성한 Rdd에 연주차 정보가 52보다 큰 값은 제거하는 로직직
    var filteredRdd = rawRdd.filter(x => {
      //boolean = true
      var checkValid = true
      // 찾기 : yearweek 인덱스로 주차정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt

      // 비교한 후 주차정보가 53 이상인 경우 레코드 삭제
      if (weekValue >= 53) {
        checkValid = false
      }

      checkValid
    })

    ////데이터 조회하고 싶을 때/////////////////////////////////
    //3개만 불러올 때
    filteredRdd.take(3).foreach(println)
    //다 불러 올때
    filteredRdd.collect.toArray.foreach(println)

    //필터예제2 : 상품정보가 PRODUCT1,2 인 정보만 필터링 하세요.
    //분석대상 제품군 등록
    var productArray = Array("PRODUCT1", "PRODUCT2")
    //세트 타입으로 변환
    var productSet = productArray.toSet

    var resultRdd = filteredRdd.filter(x=>{
      var checkValid = false

      //데이터 특정 행의 product 컬럼인덱스를 활용하여 데이터 대입
      var productInfo = x.getString(productNo)

      if(productSet.contains(productInfo)){
        checkValid = true
      }
      //2번째 정답
//      if((productInfo == "PRODUCT1")||
//        (productInfo == "PRODUCT2")){
//        checkValid = true
//      }
      checkValid
    })

    //////////////////////// 데이터형 변환 [RDD → Dataframe]////////////////////////////////////
//    var {DataFrame 변수명} = spark.createDataframe( {RDD명},
//      StructType(Seq( StructField( “컬럼명#1”, 데이터타입#1),
//    StructField( “컬럼명#2”, 데이터타입#2))
    val finalResultDf = spark.createDataFrame(resultRdd,
    StructType(
    Seq(
      StructField("KEYKOL", StringType),
      StructField("REGIONID", StringType),
      StructField("PRODUCT", StringType),
      StructField("YEARWEEK", StringType),
      StructField("QTY", DoubleType),
      StructField("PRODUCTNAME", StringType))))

    //////////////////////Rdd가공 : map  (Rdd정제 : filter)//////////////////////////////////////
    // 데이터형 변환 [데이터프레임 → RDD]
    //var {RDD변수명} = {RDD변수명}.map(x=>{ 컬럼정보 수정
     // Row( 컬럼, 컬럼          )        })
    // 데이터 확인
    //var {RDD변수명}.collect.foreach(println)

    ///////////////Row로 로직만든 map(이게 많이 쓰임!!!)//////////////////

    //처리로직 : MAXVALUE 이상인 건은 MAXVALUE로 치환한다.
    var MAXVALUE = 700000

    var mapRdd = rawRdd.map(x=>{
      //디버깅코드 : var x = mapRdd.filter(x=>{x.getDouble(qtyNo) > 700000 }).first
      //로직구현예정
      var org_qty = x.getDouble(qtyNo)
      var new_qty = org_qty

      if(new_qty > MAXVALUE) {
        new_qty = MAXVALUE
      }
      //출력 row 키정보, 거래량 정보, 거래량 정보_new)
      Row( x.getString(keyNo),
        x.getString(yearweekNo),
        org_qty,
        new_qty)

    })


    //////////////////일반적으로 로직만든 map(많이쓰지않음)////////////////////
    //////인덱스가 설정되있지않아 컬럼정보의 자리를 바꿀 수 없다./////////////
    var map1Rdd = rawRdd.map(x=>{
      var org_qty = x.getDouble(qtyNo)
      var new_qty = org_qty

      if(new_qty > MAXVALUE){new_qty = MAXVALUE}
      ( x.getString(keyNo),
        x.getString(yearweekNo),
        org_qty,
        new_qty)
      //x.getString(qtyNo) ==>기존값
    })
    mapRdd.collect.toArray.foreach(println)


    // map 디버깅 첫번째방법
    var mapexRdd = rawRdd
    var x = mapexRdd.first

    //map 디버깅 두번째방법
    var mapex2Rdd = rawRdd
    var x2 = mapex2Rdd.filter(x=> {
      (x.getDouble(qtyNo).toDouble > 700000)
    }).first




  }

}
