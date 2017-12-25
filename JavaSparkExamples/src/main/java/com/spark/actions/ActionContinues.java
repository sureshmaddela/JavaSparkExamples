package com.spark.actions;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ActionContinues {

	public static void main(String[] args) {
		
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Actions");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer>intRDD	=sc.parallelize(Arrays.asList(1,4,3));
		Integer	sumInt=intRDD.reduce((a,b)->a+b);
		System.out.println("The sum of all the elements of RDD using reduce is :"+sumInt);
		
		System.out.println("******************* fold **************************");
		
		JavaRDD<Integer>mulRDD	=sc.parallelize(Arrays.asList(1,4,3));
		Integer	mulInt=mulRDD.fold(2,(a,b)->a*b);
		// Explanation : first 2 * a=1,b =4  = 2*1*4 = 8
		//				 Now	2 * 8 * 3 = 48.		
		System.out.println("The sum of all the elements of RDD using reduce is :"+mulInt);
	}

}
