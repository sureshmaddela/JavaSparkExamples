package com.spark.transform;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDUnion {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDDMapTransform");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> intList = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> intRDD = sc.parallelize(intList, 2);
		JavaRDD<Integer> intRDD1 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9),1);
		
		//Union
		intRDD.union(intRDD1).collect().forEach(System.out::println);
		
		// Intersection
		intRDD.intersection(intRDD1).collect().forEach(System.out::println);
		
		//Distinct
		JavaRDD<Integer> distinctRDD = sc.parallelize(Arrays.asList(1,22,8,9,22,3,4,5,6,7,3,1,8,1),1);
		distinctRDD.distinct().collect().forEach(System.out::println);;
		
		// Cartesian
		
		JavaRDD<String> strRDD = sc.parallelize(Arrays.asList("A","B","C"),2);
		strRDD.cartesian(intRDD).collect().forEach(System.out::println);
		sc.close();
	
		
	}

}
