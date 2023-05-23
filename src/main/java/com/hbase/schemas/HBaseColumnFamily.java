package com.hbase.schemas;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

public class HBaseColumnFamily {

	private String columnFamilyName;

	@JsonProperty(access = Access.READ_ONLY)
	private int maxVersions;
	@JsonProperty(access = Access.READ_ONLY)
	private int timeToLive;
	@JsonProperty(access = Access.READ_ONLY)
	private boolean inMemory ;
	@JsonProperty(access = Access.READ_ONLY)
	private int blockSize;
	
	private List<String> columns ;
	@JsonIgnore
	private List<HBaseRow> rows; 

	public List<HBaseRow> getRows() {
		return rows;
	}
	public void setRows(List<HBaseRow> rows) {
		this.rows = rows;
	}
	public void addRow(HBaseRow row) {
		this.rows.add(row);
	}
	
	

	public HBaseColumnFamily() {
		super();
		// TODO Auto-generated constructor stub
	}
	public HBaseColumnFamily(String columnFamilyName) {
		super();
		this.columnFamilyName = columnFamilyName;
		this.columns = new ArrayList<>();
	}

	public HBaseColumnFamily(String columnFamilyName, int maxVersions, int timeToLive, boolean inMemory, int blockSize) {
		super();
		this.columnFamilyName = columnFamilyName;
		this.maxVersions = maxVersions;
		this.timeToLive = timeToLive;
		this.inMemory = inMemory;
		this.blockSize = blockSize;

	}
	public HBaseColumnFamily(String columnFamilyName, List<String> columns) {
		super();
		this.columnFamilyName = columnFamilyName;
		this.columns = columns;
	}
	public int getBlockSize() {
		return blockSize;
	}
	public String getColumnFamilyName() {
		return columnFamilyName;
	}
	public List<String> getColumns() {
		return columns;
	}
	public int getMaxVersions() {
		return maxVersions;
	}
	public int getTimeToLive() {
		return timeToLive;
	}
	public boolean isInMemory() {
		return inMemory;
	}
	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}
	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	public void setInMemory(boolean inMemory) {
		this.inMemory = inMemory;
	}
	public void setMaxVersions(int maxVersions) {
		this.maxVersions = maxVersions;
	}
	public void setTimeToLive(int timeToLive) {
		this.timeToLive = timeToLive;
	}


}
