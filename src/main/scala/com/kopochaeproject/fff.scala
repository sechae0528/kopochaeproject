package com.kopochaeproject

class fff {

  var groupRddMapAnswer2 = mapRdd.
    groupBy{x=> (x.getString(accountidNo),x.getString(productNo))}.
    // map/flatmap
    flatMap(x=>{
    // 기본 데이터 설정
    var key = x._1
    var data = x._2

    var size = data.size
    var sumation = data.map(x=>{x.getDouble(qtyNo)}).sum

    // 평균, 표준편차 계산
    var average = 0.0d
    if(size!=0){
      average = sumation/size
    }else{
      average = 0
    }

    // Calculate 분산
    var variance = data.map(x=>{math.pow((x.getDouble(qtyNo)-average),2)}).sum/(size-1)  ///average
    // Calculate 표준편차
    var stddev = math.sqrt(variance)
    var mapResult = data.map(x=>{
      (key,
        size,
        average,
        stddev)
    })
    mapResult
  })
}
