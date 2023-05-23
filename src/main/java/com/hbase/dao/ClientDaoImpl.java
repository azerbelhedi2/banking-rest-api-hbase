package com.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.hbase.mappers.ClientMapper;
import com.hbase.schemas.Client;

@Repository
public class ClientDaoImpl implements ClientDao {
	private static final String TABLE_NAME = "client";
	private static final String FAMILY_INFO_CLIENTS = "info_client";
	private static final String FAMILY_COMPTE_BANCAIRE = "compte_bancaire";

	@Autowired
	Configuration configuration;
	@Autowired
	Connection connection;

	Table table;

	public ClientDaoImpl() throws IOException {
		super();
		this.configuration = HBaseConfiguration.create();
		this.connection = ConnectionFactory.createConnection(configuration);
		this.table = connection.getTable(TableName.valueOf(TABLE_NAME));
	}

	@Override
	public void delete(String id) throws IOException {

		Delete delete = new Delete(Bytes.toBytes(id));
		delete.addFamily(Bytes.toBytes(FAMILY_INFO_CLIENTS));
		table.delete(delete);

	}

//	@Override
//	public List<Client> findAll() throws IOException, ClassNotFoundException, InterruptedException {
//		connection = ConnectionFactory.createConnection(configuration);
//	TableName tableName = TableName.valueOf(TABLE_NAME);
//	Table table = connection.getTable(tableName);
//		Scan scan = new Scan();
//		scan.addFamily(Bytes.toBytes(FAMILY_INFO_CLIENTS));
//
//		Job job = Job.getInstance(configuration, "ClientHBaseMapReduce");
//		job.setJarByClass(ClientDaoImpl.class);
//		job.setMapperClass(ClientMapper.class);
//		job.setOutputFormatClass(NullOutputFormat.class);
//		job.setNumReduceTasks(0);
//		job.setOutputKeyClass(ImmutableBytesWritable.class);
//		job.setOutputValueClass(Result.class);
//		TableMapReduceUtil.initTableMapperJob(table.getName(), scan, ClientMapper.class, ImmutableBytesWritable.class,
//				Result.class, job);
//		job.waitForCompletion(true);
//		Counters counters = job.getCounters();
//		Counter counter = counters.findCounter(org.apache.hadoop.mapreduce.TaskCounter.REDUCE_OUTPUT_RECORDS);
//		List<Client> clients = new ArrayList<>();
//		for (long i = 0; i < counter.getValue(); i++) {
//			Result result = job.getConfiguration().getBoolean("mapred.job.tracker", false)
//					? table.get(new Get(Bytes.toBytes(i)))
//					: table.get(new Get(Bytes.toBytes(i)));
//			Client client = new Client();
//			client.setIdClient(Bytes.toString(result.getRow()));
//			client.setLastname(
//					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("lastname"))));
//			client.setFirstname(
//					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("firstname"))));
//			client.setAdresse(
//					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("adresse"))));
//			client.setDate_inscription(Bytes
//					.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_inscription"))));
//			client.setDate_naissance(Bytes
//					.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_naissance"))));
//			client.setCin(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("cin"))));
//			client.setTel(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("tel"))));
//			client.setEmail(
//					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("email"))));
//
//			clients.add(client);
//		}
//
//		table.close();
//		connection.close();
//
//		return clients;
//	}
	@Override
	public List<Client> findAll() throws IOException {

		connection = ConnectionFactory.createConnection(configuration);
		TableName tableName = TableName.valueOf(TABLE_NAME);
		Table table = connection.getTable(tableName);

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("info_client"));
		ResultScanner scanner = table.getScanner(scan);

		List<Client> clients = new ArrayList<>();

		for (Result result : scanner) {
			Client client = new Client();
			client.setIdClient(Bytes.toString(result.getRow()));
			client.setLastname(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("lastname"))));
			client.setFirstname(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("firstname"))));
			client.setAdresse(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("adresse"))));
			client.setDate_inscription(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_inscription"))));
			client.setDate_naissance(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_naissance"))));
			client.setCin(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("cin"))));
			client.setTel(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("tel"))));
			client.setEmail(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("email"))));
			clients.add(client);
		}

		scanner.close();
		table.close();
		connection.close();

		return clients;
	}

	@Override
	public Client findById(String id) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(configuration)) {
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			Get get = new Get(Bytes.toBytes(id));
			get.addFamily(Bytes.toBytes(FAMILY_INFO_CLIENTS));
			Result result = table.get(get);
			Client client = new Client();
			client.setIdClient(Bytes.toString(result.getRow()));
			client.setLastname(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("lastname"))));
			client.setFirstname(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("firstname"))));
			client.setAdresse(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("adresse"))));
			client.setEmail(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("email"))));
			client.setCin(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("cin"))));
			client.setTel(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("tel"))));
			client.setDate_inscription(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_inscription"))));
			client.setDate_naissance(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_naissance"))));
			// client.setSolde(Bytes.toDouble(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE),
			// Bytes.toBytes("solde"))));

			table.close();
			return client;
		}
	}

	@Override
	public Client save(Client client) throws IOException {

		Put put = new Put(Bytes.toBytes(client.getIdClient()));
		put.addColumn(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("lastname"),
				Bytes.toBytes(client.getLastname()));
		put.addColumn(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("firstname"),
				Bytes.toBytes(client.getFirstname()));
		put.addColumn(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("adresse"), Bytes.toBytes(client.getAdresse()));
		put.addColumn(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_inscription"),
				Bytes.toBytes(client.getDate_inscription()));
		put.addColumn(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_naissance"),
				Bytes.toBytes(client.getDate_naissance()));
		put.addColumn(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("cin"), Bytes.toBytes(client.getCin()));
		put.addColumn(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("tel"), Bytes.toBytes(client.getTel()));
		put.addColumn(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("email"), Bytes.toBytes(client.getEmail()));
		// put.addColumn(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("type"),
		// Bytes.toBytes(client.getType()));
		// put.addColumn(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("solde"),
		// Bytes.toBytes(client.getSolde()));

		table.put(put);
		table.close();
		connection.close();
		return client;
	}
	@Override
	public List<Client> findAll(int pageSize, String startRow) throws IOException {

	    connection = ConnectionFactory.createConnection(configuration);
	    TableName tableName = TableName.valueOf(TABLE_NAME);
	    Table table = connection.getTable(tableName);
	    Scan scan = new Scan();
	    scan.addFamily(Bytes.toBytes("info_client"));
	    scan.setCaching(pageSize);
	    if (startRow != null) {
	        scan.withStartRow(Bytes.toBytes(startRow));
	    }
	    ResultScanner scanner = table.getScanner(scan);

	    List<Client> clients = new ArrayList<>();

	    int count = 0;
	    for (Result result : scanner) {
	        Client client = new Client();
	        client.setIdClient(Bytes.toString(result.getRow()));
	        client.setLastname(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("lastname"))));
	        client.setFirstname(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("firstname"))));
	        client.setAdresse(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("adresse"))));
	        client.setDate_inscription(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_inscription"))));
	        client.setDate_naissance(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_naissance"))));
	        client.setCin(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("cin"))));
	        client.setTel(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("tel"))));
	        client.setEmail(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("email"))));
	        clients.add(client);

	        count++;
	        if (count >= pageSize) {
	            break;  // Sortir de la boucle après avoir atteint la taille de la page
	        }
	    }

	    scanner.close();
	    table.close();
	    connection.close();

	    return clients;
	}
}
