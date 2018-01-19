package com.wbi
import scala.math.random
import org.apache.spark.sql.SparkSession

object GetCSV {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.print("Usage: Scala WBI <Input-File> <Output-File>");
      System.exit(1);
    }

    var sp = SparkSession.builder.appName("WBI").getOrCreate()
    //    var data =sp.read.csv("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\docs\\World_Bank_Indicators.csv").rdd
    var data = sp.read.textFile(args(0)).rdd
    val result = data.flatMap { line =>
      {
        line.split(" ")
      }
    }
  }
}