package com.spark.actions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CollectAsMap {

	public static void main(String[] args) {
	
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkActions");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<String,Integer>>list=	new	ArrayList<Tuple2<String,Integer>>();
		list.add(new Tuple2<String,Integer>("a",1));
		list.add(new Tuple2<String,Integer>("b",2));
		list.add(new Tuple2<String,Integer>("c",3));
		list.add(new Tuple2<String,Integer>("a",4));
		JavaPairRDD<String,Integer>	pairRDD	=	sc.parallelizePairs(list);
		Map<String,	Integer>collectMap=pairRDD.collectAsMap(); // Override Old Value and Update map with updated values. 
		
		collectMap.entrySet().forEach(t -> System.out.println(t.getKey() + t.getValue()));

	}

}
