package com.spark.transform;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapTransformations {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDDMapTransform");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> intList = Arrays.asList(1,2,3,4,5,6,7,8,9);
		JavaRDD<Integer> intRDD = sc.parallelize(intList, 2);// second argument is Number of Partition or slices.
		// RDD can be used again for different transformations.
		intRDD.map(x -> x + 1).collect().forEach(System.out::println);
		
		System.out.println("*********************************");
		intRDD.filter(x -> x%2 ==0).map(x -> x + 1).collect().forEach(System.out::println);
		sc.close();
		
	}

}
