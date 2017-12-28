package com.spark.datasetExample;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.commons.Employee;

public class SparkDataset {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("Spark Dataset Example").getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		JavaRDD<Employee> employeeRDD = jsc.parallelize(Arrays.asList(new Employee( 1,"Foo"),new Employee( 2,"Bar")));
		Dataset<Employee> datasetEmp = sparkSession.createDataset(employeeRDD.rdd(), org.apache.spark.sql.Encoders.bean(Employee.class));
		Dataset<Employee> filter = datasetEmp.filter(emp->emp.getId()>1);
	
		// or
		
		Dataset<Row> dfEmp = sparkSession.createDataFrame(employeeRDD, Employee.class);
		System.out.println("Employee Dataset.");
		dfEmp.show();
		
		System.out.println("Printing schema...");
		dfEmp.printSchema();
		

		//1.
		datasetEmp.filter(emp -> emp.getId()>1).show();
		
		//2.
		datasetEmp.filter("Id > 1").show();
		
		//3. DSL
		datasetEmp.filter(col("Id").gt(1)).show();
		
	}

}
