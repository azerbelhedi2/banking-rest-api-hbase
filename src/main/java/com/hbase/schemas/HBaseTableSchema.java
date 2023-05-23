package com.hbase.schemas;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

public class HBaseTableSchema {


	private String tableName;


	private List<HBaseColumnFamily> columnsFamilies;
public HBaseTableSchema() {
		super();
		// TODO Auto-generated constructor stub
	}
	public HBaseTableSchema(String tableName) {
		super();
		this.tableName = tableName;
		this.columnsFamilies = new ArrayList<>();

	}
	public HBaseTableSchema(String tableName, List<HBaseColumnFamily> columnsFamilies) {
		super();
		this.tableName = tableName;
		this.columnsFamilies = columnsFamilies;

	}
	public void addToColumnFamilies(HBaseColumnFamily columnfamily) {
		this.columnsFamilies.add(columnfamily);
	}
	public void addTolistFamily(String item) {
		this.columnsFamilies.add(new HBaseColumnFamily(item));
	}
	public List<HBaseColumnFamily> getColumnsFamilies() {
		return columnsFamilies;
	}
	//
//	@JsonProperty(access = Access.READ_ONLY)
//	private int numberOfColumnFamily = this.columnsFamilies.size();
	public String getTableName() {
		return tableName;
	}
	public void setColumnsFamilies(List<HBaseColumnFamily> columnsFamilies) {
		this.columnsFamilies = columnsFamilies;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}



	public String toJson() {
	        Gson gson = new Gson();
	        return gson.toJson(this);
	    }
}
