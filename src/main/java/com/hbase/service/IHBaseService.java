package com.hbase.service;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.hbase.schemas.HBaseColumnFamily;
import com.hbase.schemas.HBaseStatus;
import com.hbase.schemas.HBaseTableSchema;
import com.hbase.schemas.Solde;

public interface IHBaseService {

	public void addRowToHBaseTable(String tableName, String columnFamily, String qualifier, String value, String rowKey)
			throws IOException;
	public void createTable(HBaseTableSchema hbaseTable) throws IOException ;
	public HBaseTableSchema describeTable(String hbaseTable) throws IOException ;
	public void dropTable(String hbase)throws IOException;
	public HBaseStatus getHbaseStatus()throws IOException ;
	public List<HBaseTableSchema> getHBaseTablesDetails() throws IOException;
	public List<String> getTables()throws IOException  ;
	public void insertCsvData(String fileName, String tableName, String columnFamilyName) throws IOException ;
	public void loadCsvFileToHBase(String tableName, String columnFamily, File csvFile) throws IOException ;
	public List<String>  listColumns(String tableName, String columnFamily) throws IOException ;
	
}
