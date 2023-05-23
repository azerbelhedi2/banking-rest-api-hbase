package com.hbase.service;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hbase.dao.ClientDao;
import com.hbase.schemas.Client;

@Service
public class ClientService implements IClientService {

	@Autowired
	ClientDao clientDao;

	@Override
	public Client addClient(Client client) throws IOException {
		return clientDao.save(client);

	}

	@Override
	public void deleteClient(String id) throws IOException {
		clientDao.delete(id);

	}

	@Override
	public List<Client> findAllClient() throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		return clientDao.findAll();
	}

	@Override
	public Client findClientById(String id) throws IOException {
		// TODO Auto-generated method stub
		return clientDao.findById(id);
	}

	@Override
	public Client updateClient(Client client) throws IOException {
		// TODO Auto-generated method stub
		 return clientDao.save(client);

	}

	@Override
	public List<Client> findAll(int pageSize, String startRow) throws IOException {
		// TODO Auto-generated method stub
		return clientDao.findAll(pageSize, startRow);
	}

}
