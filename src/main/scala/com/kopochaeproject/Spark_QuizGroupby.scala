package com.kopochaeproject

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import edu.princeton.cs.introcs.StdStats

object Spark_QuizGroupby {
  //https://introcs.cs.princeton.edu/java/22library/StdStats.java.html

  var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

  var staticUrl = "jdbc:oracle:thin:@192.168.0.10:1521/XE"
  staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"

  val selloutDataFromOracle = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

  selloutDataFromOracle.createOrReplaceTempView("keydata")

  println(selloutDataFromOracle.show())
  println("oracle ok")

  var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
    "a.regionid as accountid, " +
    "a.product, " +
    "a.yearweek, " +
    "cast(a.qty as String) as qty, " +
    "'test' as productname from keydata a" +
    " where 1=1" +
    " and regionid = 'A01' " +
    " and product in ('PRODUCT1','PRODUCT2') ")


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
  var filterRdd = rawRdd.filter(x => {

    // Data comes in line by line
    var checkValid = true
    // Assign yearweek information to variables
    var week = x.getString(yearweekNo).substring(4, 6).toInt
    // Assign abnormal to variables
    var standardWeek = 52

    // filtering
    if (week > standardWeek) {
      checkValid = false
    }
    checkValid
  })

  // key, account, product, yearweek, qty, productname
  var mapRdd = filterRdd.map(x => {
    var qty = x.getString(qtyNo).toDouble
    var maxValue = 700000
    if (qty > 700000) {
      qty = 700000
    }
    Row(x.getString(keyNo),
      x.getString(accountidNo),
      x.getString(productNo),
      x.getString(yearweekNo),
      qty, //x.getString(qtyNo),
      x.getString(productnameNo))
  })

  var gorupRddMapTest = mapRdd.
    groupBy(x=>{
      (x.getString(keyNo), x.getString(accountidNo))
    }).
    flatMap()

}
