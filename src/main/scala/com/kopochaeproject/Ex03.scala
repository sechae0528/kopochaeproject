package com.kopochaeproject

import org.apache.spark.sql.SparkSession

object Ex03 {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("hkProject").
//      config("spark.master", "local").
//      getOrCreate()

    def discountedPrice(price:Double, rate:Double) :Double =  {
      var discount = price * rate
      var returnValue = price - discount
      returnValue
    }

    var orgRate = 0.2
    var orgPrice = 2000
    var newrPrice = discountedPrice(orgPrice, orgRate)
    println(newrPrice)


    def roundedPrice(price:Double) :Double = {
      var roundPrice = Math.round(price*100d)/100d
      roundPrice
    }

    def roundSE(targetValue: Double, sequence:Int) : Double = {
      var multiValue = Math.pow(10,sequence)
      var returnValue = Math.round(targetValue*multiValue)/multiValue
      returnValue
    }
    var a= 15.2
    var b = 13.2
    var c= 4.3
    var LimitedValue = 100.0
    var ra = roundSE(a,2)
    var rb = roundSE(b,2)
    var rc = roundSE(c,2)

    var error =roundSE((LimitedValue-ra+rb+rc),2)
    var ra2 = ra + error
    var sum = ra2+ rb + rc
    println(sum)

  }

}
