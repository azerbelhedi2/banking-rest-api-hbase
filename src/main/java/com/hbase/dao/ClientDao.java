package com.hbase.dao;

import java.io.IOException;
import java.util.List;

import com.hbase.schemas.Client;

public interface ClientDao {


		public void delete(String id)  throws IOException ;
		public List<Client> findAll() throws IOException , ClassNotFoundException, InterruptedException;
		public Client findById(String id)  throws IOException ;
		public List<Client> findAll(int pageSize, String startRow) throws IOException ;
		public Client save(Client client)  throws IOException ;
}
