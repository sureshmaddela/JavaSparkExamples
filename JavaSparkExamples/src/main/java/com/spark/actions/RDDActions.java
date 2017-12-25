package com.spark.actions;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDActions {

	public static void main(String[] args) {
		
	SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTransformations");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(11,23,4,64,85,96,87,38,92),5);

	Integer	maxVal=rdd.max(Comparator.naturalOrder());
	Integer	minVal=rdd.min(Comparator.naturalOrder());
	
	System.out.println("Max : "+ maxVal + " " + "Min : " +minVal);
	
	Integer	first=rdd.first(); // returns first Element of the RDD
	System.out.println("The	first	value	of	RDD	is	"+first);
	
	rdd.take(3).forEach(System.out::println); // Returns first 3 elements of the RDD
	rdd.takeOrdered(3).forEach(System.out::println); // Returns first 3 elements of the RDD in sorted Order
	rdd.takeOrdered((int) rdd.count()).forEach(System.out::println); // One can sort List using it by passing the argument as RDD count. TypeCasting is required. from Long to Int.
	
	rdd.takeSample(true,12).forEach(System.out::println); // when set to True, Random elements picked from RDD
	
	System.out.println("******** Top **********");
	rdd.top(2).forEach(System.out::println); // Returns Top 2 maximum elements from RDD. // where 2 is the number of returened elements.
	
	rdd.top(2,Comparator.reverseOrder()).forEach(System.out::println); // Returns Top 2 minimum Elements from RDD. // or you can write custom Comparator.
	
	
	}

}
