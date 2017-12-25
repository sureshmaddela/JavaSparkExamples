package com.spark.storage;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import com.spark.commons.Movie;

public class CSVOperations {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("SparkSession CSV RDD APP")
				.config("spark.cassandra.connection.host", "localhost").getOrCreate();

		JavaRDD<Movie> moviesRDD = sparkSession.read().textFile("/home/vidit/movies.csv").javaRDD()
				.filter(str -> !(null == str) && !(str.length() == 0) && !str.contains("movieId"))
				.map(str -> Movie.parseRating(str));
		moviesRDD.collect().forEach(t -> System.out.println(t.getMovieId() + "," + t.getTitle() + "," + t.getGenre()));

	}

}
