package com.hbase.schemas;

public class CsvData {

    private String fileName;
    private String tableName;
    private String columnFamilyName;
	public String getColumnFamilyName() {
		return columnFamilyName;
	}
	public String getFileName() {
		return fileName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


}