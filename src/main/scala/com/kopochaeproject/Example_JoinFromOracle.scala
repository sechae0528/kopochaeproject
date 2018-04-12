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

    var resultDf = spark.sql("select " +
      "concat(a.regionid,'_',a.product) AS KEYCOL, "+
      "a.regionid AS REGIONID, " +
      "a.product AS PRODUCT, " +
      "a.yearweek AS YEARWEEK, " +
      "cast(qty as double) AS QTY, " +
      "b.productname AS PRODUCTNAME " +
      "from selloutTable A " +
      "left join mstTable B " +
      "on a.product = b.productid")


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


    resultDf.
      coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("charset", "ISO-8859-1").
      option("header", "true"). // 헤더 유/무
      save("e:/resultdf2.csv")
    //ISO-8859-1 ISO-8859-1

    resultDf.rdd.coalesce(1).map { x =>x.mkString(",") }.saveAsTextFile("e:/test/test3.csv")


  }
}
