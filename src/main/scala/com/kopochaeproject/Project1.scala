package com.kopochaeproject

object Project1 {
  def main(args: Array[String]): Unit = {
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Library Definition ////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    import edu.princeton.cs.introcs.StdStats
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

    //   1. Array : 이동평균
    def movingAverage(targetData: Array[Double], myorder: Int): Array[Double] = {
      val length = targetData.size
      if (myorder > length || myorder <= 2) {
        throw new IllegalArgumentException
      } else {
        var maResult = targetData.sliding(myorder).map(x=>{x.sum}).map(x=>{ x/myorder })

        if (myorder % 2 == 0) { //짝수일 경우는 쓰지않지만 예외처리로서 사용
          maResult = maResult.sliding(2).map(x=>{x.sum}).map(x=>{ x/2 })
        }
        maResult.toArray
      }
    }

    // 2. 변동률 계산(표준편차)
    def stdevDf(targetData: Array[Double], myorder: Int): Array[Double] = {
      val length = targetData.size
      if (myorder > length || myorder <= 2) {
        throw new IllegalArgumentException
      } else {
        var maResult = targetData.sliding(myorder).map(x => {x.sum}).map(x => {x / myorder})
        var maResult1 = targetData.sliding(myorder).toArray
        var stddev1 = new ArrayBuffer[Double]

        for (i <- 0 until maResult1.length){
          var maResult2 = maResult1(i)
          var stddev = StdStats.stddev(maResult2)
          stddev1.append(stddev)
        }

        if (myorder % 2 == 0) { //짝수일 경우는 쓰지않지만 예외처리로서 사용
          maResult = maResult.sliding(2).map(x => {x.sum}).map(x => {x / 2})
          for (i <- 0 until maResult1.length){
            var maResult2 = maResult1(i)
            var stddev = StdStats.stddev(maResult2)
            stddev1.append(stddev)
          }
        }
        stddev1.toArray
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
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    //staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
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

    //(1)음수(반품)는 0으로 고정
    var rawData = spark.sql("select regionid as regionid," +
      "a.product as product," +
      "a.yearweek as yearweek," +
      "cast(a.qty as String) as qty, "+
      "cast(case When qty <= 0 then 1 else qty end as String) as qty_new " +
      "from keydata a" )

    rawData.show(2)

    //(2)인덱스설정
    var rawDataColumns = rawData.columns.map(x=>{x.toLowerCase()})
    var regionidNo = rawDataColumns.indexOf("regionid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var qtyNewNo = rawDataColumns.indexOf("qty_new")

    var rawRdd = rawData.rdd

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // The abnormal value is refined using the normal information

    //(3) 52주차가 넘어가면 제거, 52주차까지만 필터링
    var filterEx1Rdd = rawRdd.filter(x=> {

      // Data comes in line by line
      var checkValid = true
      // Assign yearweek information to variables
      var week = x.getString(yearweekNo).substring(4, 6).toInt
      // Assign abnormal to variables
      var standardWeek = 52

      if(week > standardWeek){
        checkValid = false
      }
      checkValid
    })

//    var productList = Array("PRODUCT1","PRODUCT2").toSet
//
//    var filterRdd = filterEx1Rdd.filter(x=> {
//      productList.contains(x.getString(productNo))
//    })


    // (4) qty, qty_new 형변환 : double
    var mapRdd = filterEx1Rdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var qty_new = x.getString(qtyNewNo).toDouble

      Row( x.getString(regionidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        qty, //x.getString(qtyNo),
        qty_new)
    })

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Processing        ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // Distributed processing for each analytics key value (regionid, product)
    // 1. Sort Data
    // 2. Calculate moving average
    // 3. Generate values for week (out of moving average)
    // 4. Merge all data-set for moving average
    // 5. Generate final-result
    // (key, account, product, yearweek, qty, productname)
    var groupRddMapExp1 = mapRdd.
      groupBy(x=>{ (x.getString(regionidNo), x.getString(productNo)) }).
      flatMap(x=>{

        var key = x._1
        var data = x._2

        // 1. Sort Data
        var sortedData = data.toSeq.sortBy(x=>{x.getString(yearweekNo).toInt}) //연주차별로 순서대로 order by해주는곳
        var sortedDataIndex = sortedData.zipWithIndex

        var sortedVolume = sortedData.map(x=>{ (x.getDouble(qtyNewNo))}).toArray
        var sortedVolumeIndex = sortedVolume.zipWithIndex

        //zipwithindex는 데이터가 있고 그 뒤에 인덱스를 다시 붙이는 작업
        //11,0
        //123,1
        //43,2
        //22,3

        var scope = 13
        var scope1 = 5
        var subScope = (scope.toDouble / 2.0).floor.toInt
        var subScope1 = (scope1.toDouble / 2.0).floor.toInt

        // 2. Calculate moving average
        var movingResult = movingAverage(sortedVolume,scope)

        //변동률구하기 : 표준편차
        var stdevResult = stdevDf(sortedVolume,scope1)

        // 3. Generate value for weeks (out of moving average)
        var preMAArray = new ArrayBuffer[Double] //Array는 정해진 범위안에서만 가능 Buffer은 배열을 계속 추가할 수있다.
        var postMAArray = new ArrayBuffer[Double]

        var preMAArray1 = new ArrayBuffer[Double]
        var postMAArray1 = new ArrayBuffer[Double]

        var lastIndex = sortedVolumeIndex.size-1

        for (index <- 0 until subScope) {
          var scopedDataFirst = sortedVolumeIndex.
            filter(x => x._2 >= 0 && x._2 <= (index + subScope)).
            map(x => {x._1})

          var scopedSum = scopedDataFirst.sum
          var scopedSize = scopedDataFirst.size
          var scopedAverage = scopedSum/scopedSize
          preMAArray.append(scopedAverage)

          var scopedDataLast = sortedVolumeIndex.
            filter(x => { (  (x._2 >= (lastIndex - subScope - index)) &&
              (x._2 <= lastIndex)    ) }).
            map(x => {x._1})
          var secondScopedSum = scopedDataLast.sum
          var secondScopedSize = scopedDataLast.size
          var secondScopedAverage = secondScopedSum/secondScopedSize
          postMAArray.append(secondScopedAverage)
        }

        for (index <- 0 until subScope1) {
          var scopedDataFirst = sortedVolumeIndex.
            filter(x => x._2 >= 0 && x._2 <= (index + subScope1)).
            map(x => {x._1})

          var scopedSum = scopedDataFirst.sum
          var scopedSize = scopedDataFirst.size
          var scopedAverage = scopedSum/scopedSize
          preMAArray1.append(scopedAverage)

          var scopedDataLast = sortedVolumeIndex.
            filter(x => { (  (x._2 >= (lastIndex - subScope1 - index)) &&
              (x._2 <= lastIndex)    ) }).
            map(x => {x._1})
          var secondScopedSum = scopedDataLast.sum
          var secondScopedSize = scopedDataLast.size
          var secondScopedAverage = secondScopedSum/secondScopedSize
          postMAArray1.append(secondScopedAverage)
        }

        // 4. Merge all data-set for moving average
        var maResult = (preMAArray++movingResult++postMAArray.reverse).zipWithIndex

        var maResult1 = (preMAArray1++stdevResult++postMAArray1.reverse).zipWithIndex

        //zip() : zip안에 오는 데이터 값을 앞에 있는 데이터에 붙여서 보여준다.

        // 5. Generate final-result
        var finalResult = sortedDataIndex.
          zip(maResult).
          map(x=>{

            var regionid = x._1._1.getString(regionidNo)
            var product = x._1._1.getString(productNo)
            var yearweek = x._1._1.getString(yearweekNo)
            var volume = x._1._1.getDouble(qtyNo)
            var volume_new = x._1._1.getDouble(qtyNewNo)
            var movingValue = x._2._1
            var ratio = 1.0d
            if(movingValue != 0.0d){
              ratio = volume_new / movingValue
            }else if(movingValue == 0.0d){
              movingValue = 1.0d
              ratio = volume_new / movingValue
            }
            var week = yearweek.substring(4,6)
            Row(regionid, product, yearweek, week, volume.toString, volume_new.toString, movingValue.toString, ratio.toString)
          })

        var finalResult2 = finalResult.
          zip(maResult1).
          map(x=>{
            var regionid = x._1.getString(regionidNo)
            var product = x._1.getString(productNo)
            var yearweek = x._1.getString(yearweekNo)
            var volume = x._1.getString(qtyNo)
            var volume_new = x._1.getString(qtyNewNo)
            var movingValue = x._1.getString(6)
            var stddValue = x._2._1
            var ratio = 1.0d
            if(stddValue != 0.0d){
              ratio = volume_new.toDouble / stddValue
            }else if(stddValue == 0.0d){
              stddValue = 1.0d
              ratio = volume_new.toDouble / stddValue
            }
            var week = yearweek.substring(4,6)
            Row(regionid, product, yearweek, week, volume.toString, volume_new.toString, movingValue.toString, stddValue.toString, ratio.toString)
          })
        finalResult2
      })

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data coonversion (RDD -> Dataframe) ///////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //volume = qty(실제판매량), volume = qty_new(실제판매량0처리), std(표준편차) : 변동률, MA: 이동평균(판매추세량), RATIO : 안정된 시장(QTY_NEW/MA)
    //넣어야할 것 : upper_bound: 상한, lower bound: 하한, MA_NEW : 정제된 판매량, SMOOTH: 스무딩처리 값, RATIO_1: (QTY_NEW/SMOOTH), RATIO_2: (MA_NEW/SMOOTH)
    val middleResult = spark.createDataFrame(groupRddMapExp1,
      StructType(Seq(StructField("REGIONID", StringType),
        StructField("PRODUCT", StringType),
        StructField("YEARWEEK", StringType),
        StructField("WEEK", StringType),
        StructField("VOLUME", StringType),
        StructField("VOLUME_NEW", StringType),
        StructField("MA", StringType),
        StructField("STD", StringType),
        StructField("RATIO", StringType))))

    middleResult.createOrReplaceTempView("middleTable")

    var finalResultDf = spark.sqlContext.sql("select REGIONID, PRODUCT, WEEK, AVG(VOLUME_NEW) AS AVG_VOLUME_NEW, AVG(MA) AS AVG_MA, AVG(STD) AS STD," +
      " AVG(RATIO) AS AVG_RATIO from middleTable group by REGIONID, PRODUCT, WEEK")

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Unloading        ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // File type
    //    finalResultDf.
    //      coalesce(1). // 파일개수
    //      write.format("csv").  // 저장포맷
    //      mode("overwrite"). // 저장모드 append/overwrite
    //      save("season_result") // 저장파일명
    //    println("spark test completed")
    // Database type
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "kopo_project_채성은"

    //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"

    finalResultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)
    println("Seasonality model completed, Data Inserted in Oracle DB")
  }

}
