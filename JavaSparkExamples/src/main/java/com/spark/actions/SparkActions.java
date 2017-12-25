package com.spark.actions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkActions {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDDActions");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1,2,3,48,75,6,7,8,90), 2);
		
		System.out.println(intRDD.count());
		
		JavaPairRDD<String,Integer> resultRDD = intRDD.mapToPair(t -> (t%2 == 0) ? new Tuple2<String,Integer>("Even", t) : new Tuple2<String,Integer>("Odd", t));
		resultRDD.collect().forEach(System.out::println);
		Map<String, Long> countByKeyMap= resultRDD.countByKey();
		
		countByKeyMap.entrySet().forEach(t -> System.out.println(t.getKey() + " " + t.getValue()));
	}

}
