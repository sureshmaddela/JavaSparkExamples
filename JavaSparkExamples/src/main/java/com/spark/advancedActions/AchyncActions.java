package com.spark.advancedActions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class AchyncActions {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		

	}

}
