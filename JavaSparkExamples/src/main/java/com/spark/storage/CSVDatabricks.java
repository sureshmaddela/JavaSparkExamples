package com.spark.storage;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVDatabricks {

	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkSession.builder().master("local").appName("CSV DataBricks")
				.config("spark.cassandra.connection.host", "localhost").getOrCreate();

		
		
		Dataset<Row> csv_read = sparkSession.read()
				.format("com.databricks.spark.csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load("movies.csv");

		csv_read.show();
		
		csv_read.write()
		.format("com.databricks.spark.csv")
		.option("header","true").mode("overwrite")
		.option("codec","org.apache.hadoop.io.compress.GzipCodec")
		.save("/home/vidit/Desktop/newMovies.csv");

		
	}

}
