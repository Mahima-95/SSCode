package com.settopbox.analysis

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI7_1 {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("set top box").master("local").getOrCreate();
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Setup box Analysis\\Set_Top_Box_Data.txt").rdd

    val result = data.map(data => {
      val tokens = data.split("\\^")
      (tokens, data)
    }).filter(data => {
      data._1(2).equals("115").||(data._1(2).equals("118"))
    }).filter(data => {
      data._1(4).contains("n=\"DurationSecs\"").&&(data._1(4).contains("n=\"ProgramId\""))
    }).map(data => {
      val xml = XML.loadString(data._1(4))
      val innerTags = xml \\ "nv"
      print(innerTags)

      print("last" + innerTags.last)
      var durationSecs: Long = 0
      var programId: String = null
      if ((!innerTags.last.isEmpty).&&((!innerTags.theSeq(1).isEmpty))) {
        programId = innerTags.theSeq(1).attribute("v").get.toString()
        durationSecs = innerTags.last.attribute("v").get.toString().toLong
      }
      print(durationSecs)
      (programId, durationSecs)
    }).groupByKey().sortByKey(false)
    result.foreach(println)
    println(result.count())
    spark.stop
  }

}