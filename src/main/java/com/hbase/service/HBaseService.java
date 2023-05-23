package com.hbase.service;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hbase.dao.HBaseDao;
import com.hbase.schemas.HBaseColumnFamily;
import com.hbase.schemas.HBaseStatus;
import com.hbase.schemas.HBaseTableSchema;
import com.hbase.schemas.Solde;

@Service
public class HBaseService implements IHBaseService {

	@Autowired
	HBaseDao hbaseDao;



	@Override
	public void addRowToHBaseTable(String tableName, String columnFamily, String qualifier, String value, String rowKey)
			throws IOException {
		hbaseDao.addRowToHBaseTable(tableName, columnFamily, qualifier, value, rowKey);

	}

	@Override
	public void createTable(HBaseTableSchema hbaseTable) throws IOException {
		hbaseDao.createTable(hbaseTable.getTableName(),hbaseTable.getColumnsFamilies());

	}




	@Override
	public HBaseTableSchema describeTable(String hbaseTable) throws IOException {
		// TODO Auto-generated method stub
		return hbaseDao.describeTable(hbaseTable);
	}

	@Override
	public void dropTable(String hbase) throws IOException {
		// TODO Auto-generated method stub
		hbaseDao.dropTable(hbase);

	}

	@Override
	public HBaseStatus getHbaseStatus() throws IOException {
		return hbaseDao.getHbaseStatus();
	}

	@Override
	public List<HBaseTableSchema> getHBaseTablesDetails() throws IOException {
		// TODO Auto-generated method stub
		return hbaseDao.getHBaseTablesDetails();
	}

	@Override
	public List<String> getTables() throws IOException {
		return hbaseDao.getTables();
	}

	@Override
	public void insertCsvData(String fileName, String tableName, String columnFamilyName) throws IOException {
		hbaseDao.insertCsvData(fileName, tableName, columnFamilyName);

	}

	@Override
	public void loadCsvFileToHBase(String tableName, String columnFamily, File csvFile) throws IOException {
		// TODO Auto-generated method stub
		hbaseDao.loadCsvFileToHBase(tableName, columnFamily, csvFile);

	}

	@Override
	public List<String> listColumns(String tableName, String columnFamily) throws IOException {
		// TODO Auto-generated method stub
		return hbaseDao.listColumns(tableName, columnFamily);
	}

	
	
	



}
