package com.spark.advancedtransformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Vidit 
   26-Dec-2017 11:36:58 PM
 */
public class MapPartitions {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDDMapTransform");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10),2);

		/*instead of acting upon each element of the RDD, it acts upon each partition of the RDD.
		 * mapPartitions provide an iterator to access each element of the RDD belonging to the
		   partition.*/

		JavaRDD<Integer> jrddint = intRDD.mapPartitions(iterator -> {
			List<Integer> intList = new ArrayList<>();
			while (iterator.hasNext()) {
				intList.add(iterator.next() + 1);
			}
			return intList.iterator();
		});

		
		System.out.println("Incremented Values : ");
		jrddint.collect().forEach(System.out::println);
					//or
		intRDD.map(x-> x+1).collect().forEach(System.out::println);
		
	}

}
