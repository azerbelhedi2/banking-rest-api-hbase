package com.hbase.schemas;

import java.util.ArrayList;
import java.util.List;

public class HBaseData {
	
	private List<String> columns; 
	private List<HBaseDataRow> rows;
	public List<String> getColumns() {
		return columns;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	public List<HBaseDataRow> getRows() {
		return rows;
	}
	public void setRows(List<HBaseDataRow> rows) {
		this.rows = rows;
	}
	public HBaseData() {
		columns = new ArrayList<>();
		rows = new ArrayList<>();
	}
	
	public void addRow(HBaseDataRow row) {
		this.rows.add(row);
	}

}
