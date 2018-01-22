package com.settopbox.analysis

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object KPI7_2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("set top box").master("local").getOrCreate();
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Setup box Analysis\\Set_Top_Box_Data.txt").rdd

    val result = data.map(data => {
      val tokens = data.split("\\^")
      (tokens, data)
    }).filter(data => {
      data._1(2).equals("115") || data._1(2).equals("118")
    }).filter(data => {
      data._1(4).contains("n=\"Frequency\"")
    }).map(data => {
      val xml = XML.loadString(data._1(4))
      val innerTags = xml \\ "nv"
      print(innerTags)
      var frequency: String = null
      if (!innerTags.isEmpty) {
        frequency = innerTags.theSeq(7).attribute("v").get.toString()
      }
      (frequency, data._2)
    })
    //     .sortBy(rec => { (rec._1, rec._2) }) // it will print data like below line
    //(Once,11002^1^118^2015-06-06 01:43:32.393^<d><nv n="ExtProgramID" v="Program/FYI Television, Inc./3608031" /><nv n="ProgramId" v="00370ddf-0000-0000-0000-000000000000" /><nv n="ExtStationID" v="Station/FYI Television, Inc./25692" /><nv n="StationId" v="0000645c-0000-0000-0000-000000000000" /><nv n="UtcStartTime" v="06/08/2016 23:25:00" /><nv n="IsDynamic" v="True" /><nv n="IsRecurring" v="False" /><nv n="Frequency" v="Once" /><nv n="DurationSecs" v="7500" /></d>^0b3c0ded-a331-4200-af3a-e9dde6a51630^2016060601)
    //     result.foreach(println)
    print("Total number of devices with frequency= Once -> " + result.count() + " ")
    spark.stop
  }
}