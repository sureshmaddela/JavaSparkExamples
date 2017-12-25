package com.spark.commons;

import java.io.Serializable;
import java.sql.Timestamp;

import shaded.parquet.org.codehaus.jackson.annotate.JsonProperty;

public class Person implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@JsonProperty
	private Integer cid;
	@JsonProperty
	private String county;
	@JsonProperty
	private String firstName;
	@JsonProperty
	private String sex;
	@JsonProperty
	private String year;
	@JsonProperty
	private Timestamp dateOfBirth;
	
	Person(){};

	public Integer getCid() {
		return cid;
	}

	public void setCid(Integer cid) {
		this.cid = cid;
	}

	public String getCounty() {
		return county;
	}

	public void setCounty(String county) {
		this.county = county;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public Timestamp getDateOfBirth() {
		return dateOfBirth;
	}

	public void setDateOfBirth(Timestamp dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}

}
