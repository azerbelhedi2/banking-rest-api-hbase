package com.hbase.dao;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.hbase.schemas.HBaseColumnFamily;
import com.hbase.schemas.HBaseData;
import com.hbase.schemas.HBaseStatus;
import com.hbase.schemas.HBaseTableSchema;
import com.hbase.schemas.Solde;

public interface HBaseDao {

	public void addRowToHBaseTable( String rowKey, String tableName, String columnFamily, String qualifier, String value)
			throws IOException;
	public void createTable(String tableName,List<HBaseColumnFamily> columnFamilies) throws IOException ;
	public HBaseTableSchema describeTable(String tableName) throws IOException ;
	public void dropTable(String tableName) throws IOException;
//	public void delete(String id ) throws IOException;
//	public HBaseTableSchema findByRowId(String id) throws IOException;
//	public List<HBaseTableSchema> findAll() throws IOException;
//	public void save(HBaseTableSchema hbase) throws IOException;
	public HBaseStatus getHbaseStatus()throws IOException ;
	public List<HBaseTableSchema> getHBaseTablesDetails() throws IOException;
	public List<String> getTables()throws IOException  ;
	public void insertCsvData(String fileName, String tableName, String columnFamilyName) throws IOException ;
	public void loadCsvFileToHBase(String tableName, String columnFamily, File csvFile) throws IOException ;
	public List<String> listColumns(String tableName, String columnFamily) throws IOException ;
//	public HBaseData scanTable(String table , String columnFamily , List<String> columns) throws IOException;
	public HBaseData scanTable(String table , String columnFamily ) throws IOException;
	public void createTableWithColumns(String tableName, List<HBaseColumnFamily> columnFamilies) throws IOException;
	
}
