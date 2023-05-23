package com.hbase.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.hbase.schemas.Client;
import com.hbase.schemas.HBaseColumnFamily;
import com.hbase.schemas.HBaseData;
import com.hbase.schemas.HBaseDataRow;
import com.hbase.schemas.HBaseRow;
import com.hbase.schemas.HBaseStatus;
import com.hbase.schemas.HBaseTableSchema;
import com.hbase.schemas.Solde;



@Repository
public class HBaseDaoImpl implements HBaseDao {

	@Autowired
	Configuration configuration;
	@Autowired
	Connection connection;
	


	public HBaseDaoImpl(Configuration configuration) {
		super();
		this.configuration = configuration;
	}

	@Override
	public void addRowToHBaseTable( String rowKey ,String tableName, String columnFamily, String qualifier, String value)
			throws IOException {
		connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf(tableName));
		try {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
		} finally {
			table.close();
		}
	}

	

	@Override
	public void createTable(String tableName, List<HBaseColumnFamily> columnFamilies) throws IOException {
		connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();
		TableName table = TableName.valueOf(tableName);
		if (admin.tableExists(table)) {
			System.out.println("Table already exists");
			return;
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(table);
		for (HBaseColumnFamily family : columnFamilies) {
			tableDescriptor.addFamily(new HColumnDescriptor(family.getColumnFamilyName()));
		}
		admin.createTable(tableDescriptor);
//		for (HBaseColumnFamily family : columnFamilies) {
//			for(HBaseRow row : family.getRows()) {
//				this.addRowToHBaseTable("1", tableName, family.getColumnFamilyName(), row.getQualifer(),"AUTO_GENERATED" );
//			}
//		}
		System.out.println("Table created successfully");

	}
	@Override
	public void createTableWithColumns(String tableName, List<HBaseColumnFamily> columnFamilies) throws IOException {
		connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();
		TableName table = TableName.valueOf(tableName);
		if (admin.tableExists(table)) {
			System.out.println("Table already exists");
			return;
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(table);
		for (HBaseColumnFamily family : columnFamilies) {
			tableDescriptor.addFamily(new HColumnDescriptor(family.getColumnFamilyName()));
		}
		admin.createTable(tableDescriptor);
		for (HBaseColumnFamily family : columnFamilies) {
			for(String column : family.getColumns()) {
				this.addRowToHBaseTable("1", tableName, family.getColumnFamilyName(), column.toString(),"AUTO_GENERATED" );
			}
		}
		System.out.println("Table created successfully");

	}
	@Override
	public HBaseTableSchema describeTable(String tableName) throws IOException {
		List<HBaseColumnFamily> listFamilies = new ArrayList<>();
		try (Connection connection = ConnectionFactory.createConnection(configuration)) {
			Admin admin = connection.getAdmin();
			HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
			HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
			System.out.println("Table: " + tableName);
			  System.out.println("Number of Column Families: " + tableDescriptor.getColumnFamilies().length);
			    System.out.println("Column Families: ");
			    for (HColumnDescriptor column : tableDescriptor.getColumnFamilies()) {
			        System.out.println("\t- " + column.getNameAsString());
			    }
			HBaseTableSchema table = new HBaseTableSchema();
			table.setTableName(tableName);

			for (HColumnDescriptor cf : columnFamilies) {
//	        	listFamiliesString.add(cf.getNameAsString());
				listFamilies.add(new HBaseColumnFamily(cf.getNameAsString(),cf.getMinVersions(),cf.getTimeToLive(),cf.isInMemory(),cf.getBlocksize()));
				System.out.println("\t" + cf.getNameAsString());
				System.out.println("\t\tMax Versions: " + cf.getMaxVersions());
				System.out.println("\t\tCompression: " + cf.getCompressionType().toString());
				System.out.println("\t\tBloom Filter Type: " + cf.getBloomFilterType().toString());
				System.out.println("\t\tBlock Size: " + cf.getBlocksize());
				System.out.println("\t\tTime To Live: " + cf.getTimeToLive());
				System.out.println("\t\tIn Memory: " + cf.isInMemory());
				System.out.println("\t\tBlock Cache Enabled: " + cf.isBlockCacheEnabled());
				System.out.println("\t\tScope: " + cf.getScope());
			}
			table.setColumnsFamilies(listFamilies);
			return table;
		}
	}


	@Override
	public void dropTable(String tableName)throws
	IOException{
		// TODO Auto-generated method stub
		   connection = ConnectionFactory.createConnection(configuration);
		    Admin admin = connection.getAdmin();
		    TableName table = TableName.valueOf(tableName);
		    if (!admin.tableExists(table)) {
		        System.out.println("Table doesn't exist");
		        return;
		    }
		    admin.disableTable(table);
		    admin.deleteTable(table);
		    System.out.println("Table dropped successfully");

	}
	@Override
	public HBaseStatus getHbaseStatus() throws IOException {
	    try (Connection connection = ConnectionFactory.createConnection(configuration); Admin admin = connection.getAdmin()) {
	        HBaseStatus hbaseStatus = new HBaseStatus();
	        hbaseStatus.setHbase_version(admin.getClusterStatus().getHBaseVersion());
	        hbaseStatus.setHbase_cluster_id(admin.getClusterStatus().getClusterId());
	        hbaseStatus.setMaster(admin.getClusterStatus().getMaster().toString());
	        hbaseStatus.setBackup_master(admin.getClusterStatus().getBackupMasters().toString());
	        hbaseStatus.setServer_size(admin.getClusterStatus().getServersSize());
	        hbaseStatus.setDead_region(admin.getClusterStatus().getDeadServers());
	        hbaseStatus.setAverage_load(admin.getClusterStatus().getAverageLoad());
	        return hbaseStatus;
	    } catch (IOException e) {
	        throw new IOException("Error while getting HBase cluster status", e);
	    }
	}
	@Override
	public List<HBaseTableSchema> getHBaseTablesDetails() throws IOException {
	    List<HBaseTableSchema> tablesDetails = new ArrayList<>();

	    try {

	        connection = ConnectionFactory.createConnection(configuration);
	        Admin admin = connection.getAdmin();
	        TableName[] tableNames = admin.listTableNames();
	        for (TableName tableName : tableNames) {
	            HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
	            HBaseTableSchema tableDetails = new HBaseTableSchema(tableName.getNameAsString());
	            for (HColumnDescriptor columnDescriptor : tableDescriptor.getColumnFamilies()) {
	                String familyName = columnDescriptor.getNameAsString();
	                HBaseColumnFamily familyDetails = new HBaseColumnFamily(familyName);
	                familyDetails.setMaxVersions(columnDescriptor.getMaxVersions());
	                familyDetails.setTimeToLive(columnDescriptor.getTimeToLive());
	                familyDetails.setBlockSize(columnDescriptor.getBlocksize());
	                familyDetails.setInMemory(columnDescriptor.isInMemory());
	                tableDetails.addToColumnFamilies(familyDetails);
	            }
	            tablesDetails.add(tableDetails);
	        }
	    } finally {
	        if (connection != null) {
	            connection.close();
	        }
	    }
	    return tablesDetails;
	}

//	@Override
//	public void insertCsvData(String fileName, String tableName, String columnFamilyName) throws IOException {
//		 connection = ConnectionFactory.createConnection(configuration);
//		Admin admin = connection.getAdmin();
//		Table table = connection.getTable(TableName.valueOf(tableName));
//
//		FileSystem fs = FileSystem.get(configuration);
//		Path path = new Path(fileName);
//
//		try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
//			String row;
//			CSVReader reader = new CSVReader(new FileReader(fileName));
//			try {
//				String[] header = reader.readNext();
//			} catch (CsvValidationException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			while ((row = br.readLine()) != null) {
//				String[] values = row.split(";");
//				String rowKey = values[0];
//				Put put = new Put(Bytes.toBytes(rowKey));
//	                int j = 1 ;
//	                while(j< values.length)
//	                {
//	                	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("adresse"), Bytes.toBytes(values[j]));
//	                	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cin"), Bytes.toBytes(values[j]));
//	                	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("date_inscription"), Bytes.toBytes(values[j]));
//	                	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("date_naissance"), Bytes.toBytes(values[j]));
//	                	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("email"), Bytes.toBytes(values[j]));
//	                	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("firstname"), Bytes.toBytes(values[j]));
//	                	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("lastname"), Bytes.toBytes(values[j]));
//	                	j+=1;
//	                }
//				table.put(put);
//			}
//		} finally {
//			table.close();
//			connection.close();
//			fs.close();
//		}
//	}
	public List<String> listColumns(String table, String columnFamily) throws IOException {
		List<String> columns = new ArrayList<>() ; 
		connection = ConnectionFactory.createConnection(configuration);
		TableName tableName = TableName.valueOf(table); 
		Table hbaseTable = connection.getTable(tableName);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(columnFamily));
		ResultScanner scanner = hbaseTable.getScanner(scan);
		for (Result result : scanner) {
		columns.add("rowKey");
			for(int i = 1 ; i< scanner.next().size()+1;i++) {
				String[] parts = scanner.next().toString().split(":");
				String qualifierPart = parts[i];
				columns.add(qualifierPart.substring(0,qualifierPart.indexOf("/")));		
			}
			break;
		}
            scanner.close();
            hbaseTable.close();
    		connection.close();
            return columns;
		
        }
    

	@Override
	public List<String> getTables() throws IOException {
		List<String> tableNames = new ArrayList<>();
		connection = ConnectionFactory.createConnection(configuration);
		try {
			Admin admin = connection.getAdmin();
			try {
				TableName[] list = admin.listTableNames();
				for (TableName tableName : list) {
					tableNames.add(tableName.getNameAsString());
				}
			} finally {
				admin.close();
			}
		} finally {
			connection.close();
		}
		return tableNames;
	}

	@Override
	public void insertCsvData(String fileName, String tableName, String columnFamilyName) throws IOException {
	    Path pathToFile =  Paths.get(fileName);
	    BufferedReader br = Files.newBufferedReader( pathToFile, StandardCharsets.US_ASCII);

	    connection = ConnectionFactory.createConnection(configuration);
	    Table table = connection.getTable(TableName.valueOf(tableName));

	    String line = br.readLine();
	    while (line != null) {
	        String[] values = line.split(";");
	        Put put = new Put(Bytes.toBytes(values[0]));
	        for (int i = 1; i < values.length; i++) {
	        	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("adresse"), Bytes.toBytes(values[i]));
            	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("cin"), Bytes.toBytes(values[i]));
            	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("date_inscription"), Bytes.toBytes(values[i]));
            	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("date_naissance"), Bytes.toBytes(values[i]));
            	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("email"), Bytes.toBytes(values[i]));
            	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("firstname"), Bytes.toBytes(values[i]));
            	put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("lastname"), Bytes.toBytes(values[i]));
	        }
	        table.put(put);
	        line = br.readLine();
	    }
	    table.close();
	    connection.close();
	}

	@Override
	public void loadCsvFileToHBase(String tableName, String columnFamily, File csvFile) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(configuration);
				Table table = connection.getTable(TableName.valueOf(tableName))) {
			CSVParser parser = new CSVParser(new FileReader(csvFile), CSVFormat.DEFAULT.withHeader());
			for (CSVRecord csvRecord : parser) {
				Put put = new Put(Bytes.toBytes(csvRecord.get("rowKey")));
				for (String header : parser.getHeaderMap().keySet()) {
					put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(header),
							Bytes.toBytes(csvRecord.get(header)));
				}
				table.put(put);
			}
		}
	}

	@Override
	public HBaseData scanTable(String table, String columnFamily) throws IOException {
		// TODO Auto-generated method stub
		List<String> columns = new ArrayList<>() ; 
		connection = ConnectionFactory.createConnection(configuration);
		TableName tableName = TableName.valueOf(table); 
		Table hbaseTable = connection.getTable(tableName);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(columnFamily));
		ResultScanner scanner = hbaseTable.getScanner(scan);
		HBaseData data = new HBaseData();
		for (Result result : scanner) {
			columns.add("rowKey");
			for(int i = 1 ; i< result.size()+1;i++) {
				String[] parts = result.toString().split(":");
				String qualifierPart = parts[i];
				columns.add(qualifierPart.substring(0,qualifierPart.indexOf("/")));		
			}
			break;
		}
		data.setColumns(columns);
		for (Result result : scanner) {
			HBaseDataRow row = new HBaseDataRow();
			row.addValue(Bytes.toString(result.getRow()));
			for(int i = 1 ; i< columns.size();i++) {
				row.addValue(Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columns.get(i)))));
			}
			data.addRow(row);
		}
		scanner.close();
		hbaseTable.close();
		connection.close();
		return data;
	}

	
	


	


}
