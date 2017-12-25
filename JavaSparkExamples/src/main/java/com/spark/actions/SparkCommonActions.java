package com.spark.actions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkCommonActions {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkActions");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1, 2, 3));
		boolean isRDDEmpty = intRDD.filter(a -> a.equals(5)).isEmpty(); // IsEmpty() returns boolean.
		System.out.println("The	RDD	is	empty	::" + isRDDEmpty);

		// CollectAsMap Action performed on PairRDD

		JavaRDD<Integer> intRDDs = sc.parallelize(Arrays.asList(22, 34, 73, 55, 23, 98, 52, 42, 53, 66, 34, 77, 81), 2);

		JavaPairRDD<String, Integer> pairRDD = intRDDs.mapToPair(
				t -> (t % 2 == 0) ? new Tuple2<String, Integer>("Even", t) : new Tuple2<String, Integer>("Odd", t));

		JavaPairRDD<String,Integer>	newRDD	= sc.parallelizePairs(pairRDD.collect());
		Map<String, Integer> map = newRDD.collectAsMap(); // ColectAsMap overrides the previous duplicate value with updated values.
		map.entrySet().forEach(t -> System.out.println(t.getKey() + "" + t.getValue()));
		
		//CountByKey operated over PairRDDs
		
		System.out.println(pairRDD.countByKey());
		
		
		
		

	}

}
