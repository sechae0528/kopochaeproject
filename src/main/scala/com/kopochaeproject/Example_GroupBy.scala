package com.kopochaeproject
import org.apache.spark.sql.SparkSession;

object Example_GroupBy {
  def main(args: Array[String]): Unit = {

    // group By 연산 (데이터를 key 별로 그룹핑)
    //var {그룹RDD명} = {RDD명}.groupBy{x=>{ (key 컬럼정보) }

    var spark = SparkSession.builder().config("spark.master","local").getOrCreate()

    // oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productNameDb = "kopo_product_mst"

    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    val productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")

    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.product_name " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.product_id")

    rawData.show(2)

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("product_name")

    // (kecol, accountid, product, yearweek, qty, product_name)
    var rawRdd = rawData.rdd

    var filteredRdd = rawRdd.filter(x=>{
      // boolean = true
      var checkValid = true
      // 찾기: yearweek 인덱스로 주차정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt

      // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
      if( weekValue >= 53){
        checkValid = false
      }

      checkValid
    })


    // group By 연산 (데이터를 key 별로 그룹핑)
    //var {그룹RDD명} = {RDD명}.groupBy{x=>{ (key 컬럼정보) }


   // 실습예제 – 유형1 (중복 제거한 키 데이터의 값만 확인)
   // (kecol, accountid, product, yearweek, qty, product_name)

    //분석대상을 그룹핑한다.
    var groupRdd = filteredRdd.
      //분석대상 키 정의 (거래처, 상품)
      groupBy(x=>{ (x.getString(accountidNo),x.getString(productNo)}).
      map(x=>{
        // 그룹별 분산처리가 수행됨
        var key = x._1
        var data = x._2
        var size = x._2.map(x=>{x.getDouble(qtyNo)}).size
        (key, size)
      })














    }
  }
