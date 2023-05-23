package com.hbase.dao;

import java.io.IOException;
import java.util.List;

import com.hbase.schemas.CompteBancaire;
import com.hbase.schemas.Solde;

public interface CompteBancaireDao {
	public void delete(String idCb)  throws IOException ;
	public List<CompteBancaire> findAll() throws IOException ;
	public List<CompteBancaire> findAllCompteByIdClient(String idClient)throws IOException;
	public CompteBancaire findById(String idCb)  throws IOException ;
	public List<CompteBancaire> listCompteByIdClient(String idClient) throws IOException;
	public CompteBancaire save(CompteBancaire compte)  throws IOException ;
	public List<Solde> getHistoriqueSolde(String rowKey) throws IOException; 
	public void batchUpdateCompteBancaireSolde(List<CompteBancaire> listComptes) throws IOException;
}
