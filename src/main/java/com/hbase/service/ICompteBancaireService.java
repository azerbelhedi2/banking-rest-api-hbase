package com.hbase.service;

import java.io.IOException;
import java.util.List;

import com.hbase.schemas.CompteBancaire;
import com.hbase.schemas.Solde;

public interface ICompteBancaireService {


		public CompteBancaire createCompteBancaire(CompteBancaire compteBancaire) throws IOException;
		public void deleteCompteBancaire(String idCb) throws IOException;
		public List<CompteBancaire> findAll()throws IOException;
		public List<CompteBancaire> findAllCompteByIdClient(String idClient) throws IOException ;
		public CompteBancaire getByIdCb(String idCb) throws IOException;
		public List<Solde> getHistoriqueSolde(String rowKey) throws IOException; 
		public void batchUpdateCompteBancaireSolde(List<CompteBancaire> listComptes) throws IOException;

}
