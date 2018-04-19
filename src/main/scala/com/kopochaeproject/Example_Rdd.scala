package com.kopochaeproject

import org.apache.spark.sql.SparkSession

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

    var rawRdd = rawData.rdd

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

    //데이터 조회하고 싶을 때,
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
      if((productInfo == "PRODUCT1")||
        (productInfo == "PRODUCT2")){
        checkValid = true
      }
      checkValid
    })

  }

}
