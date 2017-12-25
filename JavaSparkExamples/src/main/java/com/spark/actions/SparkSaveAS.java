package com.spark.actions;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSaveAS {

	public static void main(String[] args) {
	
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkActions");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> jrdd = sc.parallelize(Arrays.asList("Vidit","Amit","Sumit","Rachit"));
		System.out.println("Saving as textfile at /home/vidit/Desktop/File ");
		//jrdd.saveAsTextFile("/home/vidit/Desktop/File");

		//jrdd.saveAsObjectFile("/home/vidit/Desktop/ObjectFileDir");
		//JavaRDD<String> objectRDD=	sc.objectFile("/home/vidit/Desktop/ObjectFileDir");
		//objectRDD.foreach(x->System.out.println("The elements read from	ObjectFileDir are :"+x));
		
		JavaRDD<String> data = sc.textFile("/home/vidit/data.csv");
		data.collect().forEach(System.out::println);
	}

}
