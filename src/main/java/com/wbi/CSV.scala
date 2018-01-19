package com.wbi

import scala.io.Source
import java.util.regex.Pattern

object CSV extends App {
  println("file")
  val bufferedSource = Source.fromFile("D:\\Mahima\\My Dev Space\\workspace\\WBI\\World_Bank_Indicators.csv")
  for (line <- bufferedSource.getLines) {
    val cols = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))").map(_.trim)
    // do whatever you want with the columns here
    println(s"${cols(0)} ${cols(1)} ${cols(2)} ${cols(3)} ${cols(4)} ${cols(5)} ${cols(6)} ${cols(7)} ${cols(8)} ${cols(9)} ${cols(10)}")
  }
  bufferedSource.close
}
 