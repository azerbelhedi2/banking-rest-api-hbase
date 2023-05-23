package com.hbase.schemas;

public class HBaseRow {

	private String rowKey;
	private String tableName;
	private String columnFamily;
	private String qualifer;
	private String value;

	public HBaseRow() {
		super();
		// TODO Auto-generated constructor stub
	}

	public HBaseRow(String rowKey, String columnFamily, String qualifer, String value) {
		super();
		this.rowKey = rowKey;
		this.columnFamily = columnFamily;
		this.qualifer = qualifer;
		this.value = value;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public String getQualifer() {
		return qualifer;
	}

	public String getRowKey() {
		return rowKey;
	}

	public String getTableName() {
		return tableName;
	}

	public String getValue() {
		return value;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public void setQualifer(String qualifer) {
		this.qualifer = qualifer;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
