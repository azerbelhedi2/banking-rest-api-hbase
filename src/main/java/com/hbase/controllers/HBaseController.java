package com.hbase.controllers;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.google.inject.servlet.RequestScoped;
import com.hbase.dao.HBaseDao;
import com.hbase.dao.HBaseDaoImpl;
import com.hbase.schemas.CsvData;
import com.hbase.schemas.HBaseColumnFamily;
import com.hbase.schemas.HBaseData;
import com.hbase.schemas.HBaseRow;
import com.hbase.schemas.HBaseSchemaScanner;
import com.hbase.schemas.HBaseStatus;
import com.hbase.schemas.HBaseTableSchema;
import com.hbase.service.IHBaseService;
import com.hbase.service.MapReduceService;


@RestController
@RequestMapping("/api/hbase")
public class HBaseController {

	@Autowired
	IHBaseService hbaseService;
	
	@Autowired
	HBaseDao hbaseDao;
	

	@Autowired
	MapReduceService mapReduce;

	@PostMapping("/create")
	public void createTable(@RequestBody HBaseTableSchema hbaseTable) throws IOException {

		this.hbaseService.createTable(hbaseTable);
	}
	@PostMapping("/createWithColumns")
	public void createTableWithColumns(@RequestBody HBaseTableSchema hbaseTable) throws IOException {

		this.hbaseDao.createTableWithColumns(hbaseTable.getTableName(),hbaseTable.getColumnsFamilies());
	}
	@PostMapping("/addFileToHadoopDFS")
	public void addFileToHadoopDFS(@RequestPart("file") MultipartFile file) throws URISyntaxException {
		try {
			mapReduce.addFileToHadoop(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@DeleteMapping("/table/delete/{tableName}")
	public String deleteTable(@PathVariable String tableName) throws IOException {
		try {
			hbaseService.dropTable(tableName);
			return "Table" + tableName + "Deleted";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Une Erreur c'est produite";

	}

	@GetMapping("/describeTable/{tableName}")
	public HBaseTableSchema describeTable(@PathVariable String tableName) throws IOException {
		return hbaseService.describeTable(tableName);
	}
	@PutMapping("/scanTable")
	public HBaseData scanTable(@RequestBody HBaseSchemaScanner hbase) throws IOException {
		return hbaseDao.scanTable(hbase.getTableName(),hbase.getColumnFamily());
	}
	@GetMapping("/hbaseStatus")
	public HBaseStatus getHbaseStatus() throws IOException {
		return hbaseService.getHbaseStatus();

	}

	@GetMapping("/tables/list")
	public List<HBaseTableSchema> getListTables() throws IOException {
		return hbaseService.getHBaseTablesDetails();
	}

	@GetMapping("/insertCsvToHBase")
	public void insertCsvToHBase() {
		try {
			mapReduce.importCSVToHBase();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@PostMapping("/import-csv")
	public void importCsvData(@RequestBody CsvData file) throws Exception {

		//mapReduce.insertWithMapReduce(file.getFileName());
      hbaseService.insertCsvData(file.getFileName(), file.getTableName(), file.getColumnFamilyName());
		// hbaseService.loadCsvFileToHBase(tableName, columnFamily, file);
	}

	@PostMapping("/import-csv-map-reduce")
	public void importCsvMapReduce(@RequestPart("file") MultipartFile file,@RequestParam("tableName") String tableName,@RequestParam String columnFamily) throws Exception {
			
			mapReduce.importCSVToHBaseTable(file, tableName, columnFamily);
	}
	@PostMapping("/import-csv-map-reduce-with-header")
	public void importCsvMapReduceWithheaders(@RequestPart("file") MultipartFile file,@RequestParam("tableName") String tableName,@RequestParam("columnFamily") String columnFamily) throws Exception {
			
			mapReduce.importCSVToHBaseTableWithHeader(file, tableName, columnFamily);
	}
//	 @GetMapping("/spark-count")
//    public long sparkSQL() throws IOException {
//         return hbaseDao.createHbaseTable();
//    }
	@GetMapping("/tables")
	public List<String> listTables() throws IOException {
		return hbaseService.getTables();
	}
	@GetMapping("/list-columns/{tableName}/{family}")
	public List<String>  listColumnByColumnFamily(@PathVariable String tableName, @PathVariable String family) throws IOException {
		return hbaseService.listColumns(tableName, family);
	}
	@PutMapping("/put")
	public HBaseRow insertRow(@RequestBody HBaseRow row) throws IOException {
		this.hbaseDao.addRowToHBaseTable(row.getRowKey(), row.getTableName(), row.getColumnFamily(), row.getQualifer(), row.getValue());
		return row ;
	}
}
