package com.kopochaeproject

import org.apache.spark.sql.SparkSession;

object Example_Join {

   val spark = SparkSession.builder().appName("hkProject").
     config("spark.master", "local").
     getOrCreate()


  var dataPath = "c:/spark/bin/data/"
  var mainFile = "kopo_channel_seasonality_ex.csv"
  var subFile = "kopo_product_mst.csv"


  // relative path
  var mainData = spark.read.format("csv").option("header", "true").load(dataPath + mainFile)
  var subData = spark.read.format("csv").option("header", "true").load(dataPath + subFile)

  mainData.createTempView("maindata")
  subData.createTempView("subdata")

 // println(mainData)


  //left join하는 방법

  spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty"+
    "from maindata a" + "left join subdata b" + "on a.productgroup = b.product_org")

  var leftJoinData = spark.sql("select a.*, b.product_new " + "from maindata a left outer join subdata b " +
    "on a.productgroup = b.product_org")










}
