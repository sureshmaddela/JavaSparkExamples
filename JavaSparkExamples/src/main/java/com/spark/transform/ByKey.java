package com.spark.transform;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ByKey {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDDMapTransform");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Using maptoPair to convert PairRDD and then applied groupByKey.
		
		JavaRDD<Integer> pairRDD = sc.parallelize(Arrays.asList(12, 31, 44, 24, 56, 77, 88, 25, 97, 34, 44), 2);
		JavaPairRDD<String, Integer> resultRDD = pairRDD.mapToPair(t -> (t % 2 == 0) ? 
													new Tuple2<String, Integer>("Even", t) 
													: new Tuple2<String, Integer>("Odd", t));
		
		//resultRDD.groupByKey().collect().forEach(System.out::println);
		
		// ReduceByKey  : sums all the Even values as well as Odd values.
		// The reduceByKey transformation will sum all the values corresponding to a key
		resultRDD.reduceByKey((a,b) -> a + b).collect().forEach(System.out::println);
	}

}
