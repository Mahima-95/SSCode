package com.stack

import org.apache.spark.sql.SparkSession

//1.Count the total number of questions in the available data-set and collect the questions id of all the questions
object KPI1 {
  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir", "D:\\Mahima\\transferData\\spark\\cdh5")
//    System.setProperty("spark.sql.warehouse.dir", "file:/D:/Mahima/transferData/spark/spark-2.0.2-bin-hadoop2.7/spark-2.0.2-bin-hadoop2.7/spark-warehouse")
    val spark = SparkSession.builder.appName("Questions Count").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd
    var result = data.filter { line => { line.trim().startsWith("<row") } }
      .filter { line => { line.contains("PostTypeId=\"1\"") } }
    result.foreach { println }
    println("Total counts--> " + result.count())
    spark.stop()
  }
}