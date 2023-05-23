package com.hbase.schemas;

import java.util.ArrayList;
import java.util.List;

public class HBaseDataRow {
	
	private List<String> values ;

	public List<String> getValues() {
		return values;
	}

	public void setValues(List<String> values) {
		this.values = values;
	}

	public HBaseDataRow() {
		values = new ArrayList<>();
	} 
	public void addValue(String value) {
		this.values.add(value);
	}
}
