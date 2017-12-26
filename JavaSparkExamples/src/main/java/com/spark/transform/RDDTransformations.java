package com.spark.transform;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

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
		
		/*
		 * JavaRDD is already a Stream. You can't apply rdd.stream(). In case of FlatMap
		 * FlatMapToPair, The operation is performed on JavaRDD So there is need to
		 * convert first Array[] or List into stream to perform map() function.
		 * FlatMap and FlatMapToPair returns  "java.utils.iterator"
		 * Arrays.asList ---> RDD ---> flatMapToPair(splitting strings in list from
		 * RDD)---> Converted into Stream() --> to Apply map(mapping into key value pair
		 * tuple)--> iterate() over each element to convert into key value tuple.
		 */
		JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("This","is an","example","of","FlatMapToPair"));
		JavaPairRDD<String,Integer> rdd = stringRDD.flatMapToPair(s->Arrays.asList(s.split(" ")).stream().map(word -> new Tuple2<String, Integer>(word, word.length())).iterator());

		System.out.println(rdd.collect());

		
		sc.close();
		
		
	}

}
