package com.spark.json;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.spark.commons.Person;

import shaded.parquet.org.codehaus.jackson.map.ObjectMapper;

public class JsonData {

	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkSession.builder().master("local").appName("CSV DataBricks")
				.config("spark.cassandra.connection.host", "localhost").getOrCreate();

		// Not working...
		
		/*RDD<String>textFile=sparkSession.sparkContext().textFile("person.json",	2);
		JavaRDD<Person>mapParser=textFile.toJavaRDD().map(v1 ->	new	ObjectMapper().readValue(v1,Person.class));
				mapParser.foreach(t	->	System.out.println(t.getFirstName() + t.getSex()));*/
		
		// Not Working
		/*Dataset<Row>json_rec=sparkSession.read().json("/home/vidit/Downloads/generated.json");
		json_rec.printSchema();
		json_rec.show();*/
		
		StructType schema = new StructType(
				new StructField[] { 
						DataTypes.createStructField("cid", DataTypes.IntegerType, true),
						DataTypes.createStructField("county", DataTypes.StringType, true),
						DataTypes.createStructField("firstName", DataTypes.StringType, true),
						DataTypes.createStructField("sex", DataTypes.StringType, true),
						DataTypes.createStructField("year", DataTypes.StringType, true),
						DataTypes.createStructField("dateOfBirth", DataTypes.TimestampType, true) });
		
				Dataset	<Row> person_mod = sparkSession.read().schema(schema).json("person.json");
				person_mod.printSchema();
				person_mod.show();



				
		


	}

}
