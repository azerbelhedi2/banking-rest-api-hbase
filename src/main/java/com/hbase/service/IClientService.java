package com.hbase.service;

import java.io.IOException;
import java.util.List;

import com.hbase.schemas.Client;

public interface IClientService {

	public Client addClient(Client client) throws IOException;
	public void deleteClient(String id) throws IOException;
	public List<Client> findAllClient() throws  IOException, ClassNotFoundException, InterruptedException;
	public Client findClientById(String id) throws IOException;
	public Client updateClient(Client client) throws IOException;
	public List<Client> findAll(int pageSize, String startRow) throws IOException ;
}
