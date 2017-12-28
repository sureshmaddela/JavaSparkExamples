package com.spark.sqlExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

public class SparkSessionInitialization {
	public static void main(String[] args) {
	SparkSession sparkSession = SparkSession.builder().master("local").config("spark.driver.memory", "2G").appName("Spark Session Example").getOrCreate();	

	SparkContext sc = sparkSession.sparkContext();
	SparkConf conf = sparkSession.sparkContext().getConf();	

	Tuple2<String, String>[] all = sc.conf().getAll();
	//System.out.println(JavaConverters.mapAsScalaMapConverter(all).asJava().get("spark.driver.memory"));
	
	}
}
