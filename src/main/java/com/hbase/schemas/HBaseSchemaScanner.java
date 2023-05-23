package com.hbase.schemas;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

public class HBaseSchemaScanner {

	private String tableName ; 
	private String columnFamily;
	
	@JsonProperty(access = Access.READ_ONLY)
	private List<String> columns ;
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getColumnFamily() {
		return columnFamily;
	}
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}
	public List<String> getColumns() {
		return columns;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	public HBaseSchemaScanner() {
		super();
		columns = new ArrayList<>();
	}  
	
}
