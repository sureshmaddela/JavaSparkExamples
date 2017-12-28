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

		////option("multiline",true) for multiline json files. otherwise try to keep all elements in single line in json file. Applicable for Spark = >2.2
		Dataset<Person> peopleDS = sparkSession.read().option("multiLine", true).option("mode", "PERMISSIVE").json("person.json").as(personEncoder);
		//Dataset<Row> peopleDS = sparkSession.read().option("multiLine", true).option("mode", "PERMISSIVE").json("person.json");
		peopleDS.printSchema();
		peopleDS.show();
	}

}
