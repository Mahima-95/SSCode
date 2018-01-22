package com.settopbox.analysis

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI6 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("set top box").master("local").getOrCreate();
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Setup box Analysis\\Set_Top_Box_Data.txt").rdd

    val result = data.map(data => {
      val tokens = data.split("\\^")
      (tokens, data)
    }).filter(data => {
      data._1(2).equals("107")
    }) /*.filter(data => {
      data._1(4).contains("n =\"ButtonName\"")
    })*/ .map(data => {
        val xml = XML.loadString(data._1(4))
        val innerTags = xml \\ "nv"
        print(innerTags)
        var buttonNames: String = null
        if (!innerTags.theSeq(0).isEmpty.&&(innerTags.theSeq(0).toString().contains("n =\"ButtonName\""))) {
          buttonNames = innerTags.theSeq(0).attribute("v").get.toString()
        }
        print(buttonNames)
        (buttonNames, data._2)
      })
      .groupByKey()
      //.groupBy(_._1)
      // .reduceByKey(_ + _) // we can comment this line
      .sortBy(rec => { (rec._1, rec._2) })
    //.sortByKey(false).take(5)
    result.foreach(println)
    println("ButtonName -> " +result.count())
    spark.stop
  }
}