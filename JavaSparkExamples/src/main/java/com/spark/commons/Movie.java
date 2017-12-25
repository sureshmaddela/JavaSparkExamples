package com.spark.commons;

import java.io.Serializable;
import java.util.stream.Stream;

public class Movie implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Integer movieId;
	private String title;
	private String genre;

	Movie() {
	};

	Movie(Integer movieId, String title, String genere) {

		super();
		this.movieId = movieId;
		this.title = title;
		this.genre = genere;
	}

	public Integer getMovieId() {
		return movieId;
	}

	public void setMovieId(Integer movieId) {
		this.movieId = movieId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	public static Movie parseRating(String str) {
		String[] fields = str.split(",");
		if (fields.length != 3) {
			System.out.println("The	elements are ::");
			Stream.of(fields).forEach(System.out::println);
			throw new IllegalArgumentException(
					"Each line must contain	3 fields while the current line	has	::" + fields.length);
		}
		Integer movieId = Integer.parseInt(fields[0]);
		String title = fields[1].trim();
		String genere = fields[2].trim();
		return new Movie(movieId, title, genere);
	}

}
