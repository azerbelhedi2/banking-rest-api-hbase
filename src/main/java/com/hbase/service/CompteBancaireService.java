package com.hbase.service;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hbase.dao.CompteBancaireDao;
import com.hbase.schemas.CompteBancaire;
import com.hbase.schemas.Solde;

@Service
public class CompteBancaireService implements ICompteBancaireService {

	@Autowired
	CompteBancaireDao compteBancaireDao;
	@Override
	public CompteBancaire createCompteBancaire(CompteBancaire compteBancaire) throws IOException {
		// TODO Auto-generated method stub
		return compteBancaireDao.save(compteBancaire);
	}

	@Override
	public void deleteCompteBancaire(String idCb) throws IOException {
		// TODO Auto-generated method stub
		compteBancaireDao.delete(idCb);

	}

	@Override
	public List<CompteBancaire> findAll() throws IOException {
		// TODO Auto-generated method stub
		return compteBancaireDao.findAll();
	}

	@Override
	public List<CompteBancaire> findAllCompteByIdClient(String idClient) throws IOException {
		// TODO Auto-generated method stub
		return compteBancaireDao.listCompteByIdClient(idClient);
	}

	@Override
	public CompteBancaire getByIdCb(String idCb) throws IOException {
		// TODO Auto-generated method stub
		return compteBancaireDao.findById(idCb);
	}

	@Override
	public List<Solde> getHistoriqueSolde(String rowKey) throws IOException {
		// TODO Auto-generated method stub
		return compteBancaireDao.getHistoriqueSolde(rowKey);
	}

	@Override
	public void batchUpdateCompteBancaireSolde(List<CompteBancaire> listComptes) throws IOException {
		// TODO Auto-generated method stub
		compteBancaireDao.batchUpdateCompteBancaireSolde(listComptes);
		
	}

}
