package com.spark.datasetExample;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import com.spark.commons.Person;

public class DatasetUsingEncoders {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local")
				.config("spark.driver.memory", "2G")
				.appName("Struct Type Dataset").getOrCreate();
		
		Encoder<Person> personEncoder = Encoders.bean(Person.class);

		Dataset<Person> peopleDS = sparkSession.read().json("person.json").as(personEncoder);
		peopleDS.printSchema();
		peopleDS.show();
	}

}
