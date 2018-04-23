package com.pace.logs;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LogsAnalysis {

	public static void paceLogs() {

		System.setProperty("hadoop.home.dir", "G:\\Big Data\\Hadoop\\Setup\\hadoop-2.5.0-cdh5.3.2");
		SparkConf conf = new SparkConf().setAppName("Pace Logs").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> firstRdd = sc.textFile("G:\\Big Data\\Spark\\Spark Programs\\Dataset/Pace_Logs.csv");
		JavaRDD<Tuple2<Time, String>> result = firstRdd.map(f -> Arrays.asList(f.split(",")))
				.filter(line -> line.get(7).equalsIgnoreCase("productive"))
				.mapToPair(line -> new Tuple2<String, Time>(line.get(0), getTimes(line.get(3))))
				.reduceByKey((x, y) -> new Time(x.getHours() + y.getHours(), x.getMinutes() + y.getMinutes(),
						x.getSeconds() + y.getSeconds()))
				.map(item -> item.swap()).sortBy(line -> line._1, false, 2);

		result.foreach(x -> System.out.println(x));

		sc.close();
	}

	private static Time getTimes(String time) {

		SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
		Date date = null;
		Time times = null;
		try {
			date = sdf.parse(time);
			times = new Time(date.getHours(), date.getMinutes(), date.getSeconds());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return times;
	}

	public static void main(String[] args) throws ParseException {
		LogsAnalysis.paceLogs();

		// SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
		// Date ss = sdf.parse("0:00:03");
		// Date ss1 = sdf.parse("0:00:08");
		//
		// Date ssz = sdf.parse("0:00:11");
		//
		// int hrs = ss.getHours() + ss1.getHours();
		// int min = ss.getMinutes() + ss1.getMinutes();
		// int sec = ss.getSeconds() + ss1.getSeconds();
		//
		// String newTime = hrs + ":" + min + ":" + sec;
		// Time time = new Time(hrs, min, sec);
		// System.out.println(time);
		// Date ss2 = sdf.parse(newTime);
		// long d = ss.getTime() + ss1.getTime();
		// Date sumDate = new Date(d);
		// System.out.println(ss2);
		//
		// System.out.println(time.compareTo(ssz));
	}
}
