package com.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.hbase.schemas.CompteBancaire;
import com.hbase.schemas.Solde;

@Repository
public class CompteBancaireDaoImpl implements CompteBancaireDao {

	private static final String TABLE_NAME = "client";
	private static final String FAMILY_COMPTE_BANCAIRE = "compte_bancaire";

	@Autowired
	Configuration configuration;
	@Autowired
	Connection connection;

	Table table;

	public CompteBancaireDaoImpl() throws IOException {
		super();
		this.configuration = HBaseConfiguration.create();
		this.connection = ConnectionFactory.createConnection(configuration);
		this.table = connection.getTable(TableName.valueOf(TABLE_NAME));

	}

	@Override
	public void delete(String idCb) throws IOException {
		// TODO Auto-generated method stub
		Delete delete = new Delete(Bytes.toBytes(idCb));
		delete.addFamily(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE));
		table.delete(delete);
	}

	@Override
	public List<CompteBancaire> findAll() throws IOException {
		connection = ConnectionFactory.createConnection(configuration);
		TableName tableName = TableName.valueOf(TABLE_NAME);
		Table table = connection.getTable(tableName);
		Scan scan = new Scan();

		scan.addFamily(Bytes.toBytes("compte_bancaire"));
		ResultScanner scanner = table.getScanner(scan);

		List<CompteBancaire> comptes = new ArrayList<>();

		for (Result result : scanner) {
			CompteBancaire compte = new CompteBancaire();
			compte.setIdCb(Bytes.toString(result.getRow()));
			compte.setIdClient(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("idClient"))));
			compte.setDate_creation(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("date_creation"))));
			compte.setNumero_compte(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("numero_compte"))));
			compte.setSolde(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("solde"))));
			compte.setType_compte(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("type_compte"))));

			comptes.add(compte);
		}

		scanner.close();
		table.close();
		connection.close();

		return comptes;
	}

	@Override
	public List<CompteBancaire> findAllCompteByIdClient(String idClient) throws IOException {
		// TODO Auto-generated method stub
		connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("compte_bancaire"));
//	        scan.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes(FAMILY_COMPTE_BANCAIRE));
//	        scan.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes(FAMILY_COMPTE_BANCAIRE));
//	        scan.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes(FAMILY_COMPTE_BANCAIRE));
//	        scan.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes(FAMILY_COMPTE_BANCAIRE));
		scan.setRowPrefixFilter(Bytes.toBytes(idClient));

		List<CompteBancaire> comptes = new ArrayList<>();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			CompteBancaire compte = new CompteBancaire();
			compte.setIdCb(Bytes.toString(result.getRow()));
			compte.setIdClient(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("idClient"))));
			compte.setDate_creation(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("date_creation"))));
			compte.setNumero_compte(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("numero_compte"))));
			compte.setSolde(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("solde"))));
			compte.setType_compte(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("type_compte"))));

			comptes.add(compte);
		}
		return comptes;

	}

	@Override
	public CompteBancaire findById(String idCb) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(configuration)) {
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			Get get = new Get(Bytes.toBytes(idCb));
			get.addFamily(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE));
			Result result = table.get(get);
			CompteBancaire compte = new CompteBancaire();
			compte.setIdCb(Bytes.toString(result.getRow()));
			compte.setIdCb(Bytes.toString(result.getRow()));
			compte.setIdClient(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("idClient"))));
			compte.setDate_creation(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("date_creation"))));
			compte.setNumero_compte(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("numero_compte"))));
			compte.setSolde(
					Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("solde"))));
			compte.setType_compte(Bytes
					.toString(result.getValue(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("type_compte"))));
			table.close();
			return compte;
		}

	}

	@Override
	public List<CompteBancaire> listCompteByIdClient(String idClient) throws IOException {

		List<CompteBancaire> comptes = new ArrayList<>();
		connection = ConnectionFactory.createConnection(configuration);
		Scan scan = new Scan();
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("idClient"),
				CompareOp.EQUAL, Bytes.toBytes(idClient));
		scan.setFilter(filter);
		scan.addFamily(Bytes.toBytes("compte_bancaire"));

		try (Table table = connection.getTable(TableName.valueOf("client"));
				ResultScanner scanner = table.getScanner(scan)) {

			for (Result result : scanner) {
				CompteBancaire compte = new CompteBancaire();
				byte[] rowKey = result.getRow();
				compte.setIdCb(Bytes.toString(rowKey));
				compte.setIdClient(
						Bytes.toString(result.getValue(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("idClient"))));
				compte.setNumero_compte(Bytes
						.toString(result.getValue(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("numero_compte"))));
				compte.setSolde(
						Bytes.toString(result.getValue(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("solde"))));
				compte.setType_compte(Bytes
						.toString(result.getValue(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("type_compte"))));
				compte.setDate_creation(Bytes
						.toString(result.getValue(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("date_creation"))));
				comptes.add(compte);
			}
		}

		return comptes;
	}

	@Override
	public CompteBancaire save(CompteBancaire compte) throws IOException {

		connection = ConnectionFactory.createConnection(configuration);
		// TODO Auto-generated method stub
		Put put = new Put(Bytes.toBytes(compte.getIdCb()));
		put.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("idClient"),
				Bytes.toBytes(compte.getIdClient()));
		put.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("numero_compte"),
				Bytes.toBytes(compte.getNumero_compte()));
		put.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("type_compte"),
				Bytes.toBytes(compte.getType_compte()));
		put.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("solde"), Bytes.toBytes(compte.getSolde()));
		put.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("date_creation"),
				Bytes.toBytes(compte.getDate_creation()));
		table.put(put);
		table.close();
		connection.close();
		return compte;

	}

	public List<Solde> getHistoriqueSolde(String rowKey) throws IOException {

		connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf("client"));

		Scan scan = new Scan();
		scan.setRowPrefixFilter(Bytes.toBytes(rowKey));
		scan.addColumn(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("solde"));
		scan.setMaxVersions(100);

		ResultScanner scanner = table.getScanner(scan);

		List<Solde> listSoldes = new ArrayList<>();
		for (Result result : scanner) {
			List<Cell> cells = result.getColumnCells(Bytes.toBytes("compte_bancaire"), Bytes.toBytes("solde"));
			for (Cell cell : cells) {
				Solde solde = new Solde();
				solde.setTimestamp(cell.getTimestamp());
				String soldeClair = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

				solde.setValue(soldeClair);
				listSoldes.add(solde);
			}
		}

		scanner.close();
		table.close();
		connection.close();
		return listSoldes;
	}

	@Override
	public void batchUpdateCompteBancaireSolde(List<CompteBancaire> listComptes) throws IOException {
		connection = ConnectionFactory.createConnection(configuration);

		for (CompteBancaire compte : listComptes) {
			Put put = new Put(Bytes.toBytes(compte.getIdCb()));
			put.addColumn(Bytes.toBytes(FAMILY_COMPTE_BANCAIRE), Bytes.toBytes("solde"),
					Bytes.toBytes(compte.getSolde()));
			table.put(put);
		}
		table.close();
		connection.close();

	}

}
