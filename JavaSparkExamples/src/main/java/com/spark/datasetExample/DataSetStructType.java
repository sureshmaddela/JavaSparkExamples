package com.spark.datasetExample;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataSetStructType {

	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkSession.builder().master("local")
				.config("spark.driver.memory", "2G")
				.appName("Struct Type Dataset").getOrCreate();
		
		JavaRDD<String>	deptRDD	= sparkSession.sparkContext().textFile("movies.txt",1).toJavaRDD();
		//Convert	the	RDD	to	RDD<Rows> java 7
		/*JavaRDD<Row> movieRows = deptRDD.filter(str -> !str.contains("movieID")).map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			public Row call(String rowString) throws Exception {
				String[] cols = rowString.split(",");
				return RowFactory.create(cols[0].trim(), cols[1].trim(), cols[2].trim());
			}
		});*/

		//Convert the RDD<String> to RDD<Rows> Java8
		JavaRDD<Row> movieRows = deptRDD.filter(str -> !str.contains("movieID")).map(rowString -> {
			String[] cols = rowString.split(",");
	

				return RowFactory.create(cols[0].trim(), cols[1].trim(), cols[2].trim());
	
		});
		
		System.out.println("Movies Rows : ");
		//movieRows.collect().forEach(System.out::println);
		

		// Create schema
		String[] schemaArr = deptRDD.first().split(",");
		List<StructField> structFieldList = new ArrayList<>();
		for (String fieldName : schemaArr) {
			StructField structField = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			structFieldList.add(structField);
		}
		StructType schema = DataTypes.createStructType(structFieldList);
		Dataset<Row> deptDf = sparkSession.createDataFrame(movieRows, schema);
		deptDf.printSchema();
		//deptDf.show();

		deptDf.createOrReplaceTempView("movies");

		System.out.println("Temp view");
		Dataset<Row> result = sparkSession
				.sql("select count(movieId),title from movies where movieId > '19' group by title");
		result.show();

	}

}
