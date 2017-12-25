package com.spark.transform;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDTransformations {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDDTransformations");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Flatmap Example.
		JavaRDD<String> flatRDD = sc.parallelize(Arrays.asList("This","is an","example","of","FlatMap"));
		JavaRDD<String> flatResult = flatRDD.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
		flatResult.collect().forEach(System.out::println);
		
		//MaptoPair  // Here JavaPairRDD is used.
		System.out.println("*********************** Java MapToPair Example***********************");
		JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(12,31,44,24,56,77,88,25,97,34,44),2);
		JavaPairRDD<String,Integer> resultRDD = intRDD.mapToPair(t -> (t%2 == 0) ? new Tuple2<String,Integer>("Even", t) : new Tuple2<String,Integer>("Odd", t));
		resultRDD.collect().forEach(System.out::println);
		
		
		System.out.println("******************** FlatMap to Pair **********************");
		//flatRDD.flatMapToPair(t -> new Tuple2<Strint,Integer>(t,t.length()));
		// toDO
		
		
		sc.close();
		
		
	}

}
