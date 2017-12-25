package com.spark.wordcount;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountSpark {

	public static void main(String[] args) {
	

		SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> inputData = sc.textFile("/home/vidit/Desktop/word.txt");
		
		//System.out.println(inputData.collect());
		JavaPairRDD<String,Integer> wordPairs = inputData.flatMapToPair(text -> Arrays.asList(text.split(" "))
				.stream()
				.map(word -> new Tuple2<String, Integer>(word, 1)).iterator());
		JavaPairRDD<String,Integer> sortedVal = wordPairs.sortByKey();
		System.out.println(sortedVal.countByKey());
		JavaPairRDD<String,Integer> finalResult = sortedVal.reduceByKey((v1,v2) -> v1+v2);
		System.out.println(finalResult.collect());
		
		List<Tuple2<String, Integer>> keylistReduce =  finalResult.collect();
		System.out.println("Size : " + keylistReduce.size());
	}

}
