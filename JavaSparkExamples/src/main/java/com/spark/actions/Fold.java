package com.spark.actions;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Fold {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("FoldExample");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<String, Integer>("Vidit", 9000),
				new Tuple2<String, Integer>("Amit", 2000), new Tuple2<String, Integer>("Sumit", 6000));
		
		JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(list);
		//list.forEach(System.out::println);
		
		Tuple2<String, Integer> dummyVal = new Tuple2<String, Integer>("dummy",0);
		Tuple2<String, Integer> maxSalary = rdd.fold(dummyVal,(acc,employee) -> (acc._2 > employee._2) ? employee : acc );
		System.out.println(maxSalary.toString());   // TO-DO
		/*String maxSalaryEmployee = rdd.fold(0)((acc,employee) => { 
			if(acc._2 < employee._2) employee else acc})
			println("employee with maximum salary is"+maxSalaryEmployee)
		*/
	}

}
