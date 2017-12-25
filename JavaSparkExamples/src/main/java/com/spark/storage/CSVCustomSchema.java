package com.spark.storage;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVCustomSchema {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().master("local").appName("Custom Schema")
				.config("spark.cassandra.connection.host", "localhost").getOrCreate();

		StructType customSchema = new StructType(
				new StructField[] { 
						new StructField("movieId", DataTypes.LongType, true, Metadata.empty()),
						new StructField("title", DataTypes.StringType, true, Metadata.empty()),
						new StructField("genres", DataTypes.StringType, true, Metadata.empty()) });
		
		Dataset<Row> csv_custom_read = sparkSession.read()
				.format("com.databricks.spark.csv").option("header", "true")
				.schema(customSchema).load("/home/vidit/movies.csv");
		csv_custom_read.printSchema();
		csv_custom_read.show();

	}

}
