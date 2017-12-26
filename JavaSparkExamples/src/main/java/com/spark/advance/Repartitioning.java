package com.spark.advance;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Vidit 
   26-Dec-2017 11:37:12 PM
 */
public class Repartitioning {

	public static void main(String[] args) {
	
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Repartitioning");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(2,31,34,666,86,44),3);
		System.out.println("Initial Partition count : " + intRDD.getNumPartitions());
		JavaRDD<Integer> intRDDnew = intRDD.repartition(5);
		System.out.println("RePartition count : " + intRDDnew.getNumPartitions());
	}

}
