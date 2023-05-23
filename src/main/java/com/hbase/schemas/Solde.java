package com.hbase.schemas;

public class Solde {
	private long timestamp;
	private String value;
	public Solde(long timestamp, String value) {
		super();
		this.timestamp = timestamp;
		this.value = value;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public Solde() {
		super();
		// TODO Auto-generated constructor stub
	} 
	
}
