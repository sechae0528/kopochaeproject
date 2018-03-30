package com.kopochaeproject

import org.apache.spark.sql.SparkSession

object Ex02 {
  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().appName("seProject").
        config("spark.master", "local").
        getOrCreate()

//      // 접속정보 설정
//      var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
//      var staticUser = "kopo"
//      var staticPw = "kopo"
//      var selloutDb = "kopo_batch_season_mpara"
//
//      // jdbc (java database connectivity) 연결
//      val selloutDataFromPg = spark.read.format("jdbc").
//        options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
//
//      // 메모리 테이블 생성
//      selloutDataFromPg.createOrReplaceTempView("selloutTable")
//      selloutDataFromPg.show(1)
//     // println("postgres success")


//
//      // 파일설정// 파일설정
//      var staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
//      var staticUser = "root"
//      var staticPw = "P@ssw0rd"
//      var selloutDb = "KOPO_PRODUCT_VOLUME"
//
//      // jdbc (java database connectivity)
//      val selloutDataFromMysql= spark.read.format("jdbc").
//        options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load
//
//      selloutDataFromMysql.createOrReplaceTempView("selloutTable")
//      selloutDataFromMysql.show(1)
//      selloutDataFromMysql.count()



      // 파일설정
      var staticUrl = "jdbc:sqlserver://192.168.110.70;databaseName=kopo"
      var staticUser = "haiteam"
      var staticPw = "haiteam"
      var selloutDb = "dbo.KOPO_PRODUCT_VOLUME"

      // jdbc (java database connectivity) 연결
      val selloutDataFromSqlserver= spark.read.format("jdbc").
        options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

      // 메모리 테이블 생성
      selloutDataFromSqlserver.registerTempTable("selloutTable")

  }
}
