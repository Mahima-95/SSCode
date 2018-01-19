package com.stack

import org.apache.spark.sql.SparkSession
import scala.xml.XML

//3.Provide the number of posts which are questions and contains specified words in their title (like data, science, nosql, hadoop, spark)

object KPI3 {

  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "D:\\Mahima\\transferData\\spark\\cdh5")
    //  System.setProperty("spark.sql.warehouse.dir", "file:/D:/Mahima/transferData/spark/spark-2.0.2-bin-hadoop2.7/spark-2.0.2-bin-hadoop2.7/spark-warehouse")
    val spark = SparkSession.builder.appName("Questions Count").master("local").getOrCreate()

    //Read some example file to a test RDD
    val data = spark.read.textFile("D:\\Mahima\\My Dev Space\\workspace\\WBI_Scala\\HP_And_SK\\Stack Overflow Data Analysis\\Posts.xml").rdd
    val result = data.filter { line => { line.trim().startsWith("<row") }
    }
      .filter { line => { line.contains("PostTypeId=\"1\"") }
      }.flatMap { line =>
        {
          var xml = XML.loadString(line)
          xml.attributes("Title")
        }
      }.filter { line =>
        {
          line.mkString.toLowerCase().contains("science")
        }
      }
    result.foreach { println }
    println("Result Count: " + result.count())

    spark.stop
  }
}