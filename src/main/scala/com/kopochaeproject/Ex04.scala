package com.kopochaeproject

object Ex04 {
  def main(args: Array[String]): Unit = {
    def result(a:String) :(Int, Int) = {

      //var a = "2017;34"
      var target = a.split(";")
      var yearValue = target(0)
      var weekValue = target(1)
      (yearValue.toInt, weekValue.toInt)

    }
    var test = "2017;34"

    var answer = result(test)

    println(answer)
    println(answer._1)
    println(answer._2)

  }

}
