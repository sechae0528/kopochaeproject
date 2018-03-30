package com.kopochaeproject

import org.apache.spark.sql.SparkSession

object Ex01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("seProject").
      config("spark.master", "local").
      getOrCreate()

//조건문
    var score = 9
    var level = "기타"
    if(score>9) {
      level = "수"
    }else if((score>8) && (score <=9)){
      level = "우"
    }else {
      level = "양"
    }

    // 반복하기
//    var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
//    var promotionRate = 0.2
//    var priceDataSize = priceData.size
//    var i = 0
//
//    while(i < priceDataSize){
//      var promotionEffect = priceData(i) * promotionRate
//      priceData(i) = priceData(i) - promotionEffect
//      i=i+1
//      println(priceData(i))
//    }


    //    var a = 10
//    println(10)
//
//    var testArray = Array(22,33,50,70,90,100)
//    var answer = testArray.filter(x=>{x%10 == 0})
//
//    var answer1 = testArray.filter(x=> {
//      var data = x.toString
//      var dataSize = data.size
//
//      var lastChar = data.substring(dataSize -1).toString
//
//      lastChar.equalsIgnoreCase("0")
//
//    })
//
//    var arraySize = answer.size
//    for(i<-0 until arraySize){
//      println(answer(i))
//    }

    var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size

    for(i <-0 until priceDataSize){
      var promotionEffect = priceData(i) * promotionRate
      priceData(i) = priceData(i) - promotionEffect
    }








  }

}
