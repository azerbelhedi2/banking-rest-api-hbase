package com.hbase.controllers;



import java.io.IOException;
import java.util.List;

import javax.ws.rs.Path;

import org.apache.hadoop.conf.Configuration;
//import java.util.stream.Collectors;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hbase.schemas.Client;
import com.hbase.service.IClientService;




@RestController
@RequestMapping("/api/client")
public class ClientController {


	@Autowired
	IClientService clientService;
	@Autowired
	Configuration configuration;

//	@Autowired
//	SparkService sparkService;



	@PostMapping("/add")
	public Client addClient(@RequestBody Client client) {
		try {
			clientService.addClient(client);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return client;
	}

	@DeleteMapping("/delete/{id}")
	public void deleteClient(@PathVariable("id") String id) {
		try {
			clientService.deleteClient(id);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@GetMapping("/get/{id}")
	public Client getById(@PathVariable("id") String id) {
		try {
			return clientService.findClientById(id);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}
	@GetMapping("/query")
	public List<Client> QueryAllClients() {
		try {
			return clientService.findAllClient();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;


	}
	@GetMapping("/query/{pageSize}/{startRow}")
	public List<Client> queryWithaginator(@PathVariable int pageSize , @PathVariable String startRow) {
		try {
			return clientService.findAll(pageSize, startRow);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;


	}
	//	@GetMapping("/clients/{sql}")
//    public String getAllClients(@PathVariable("sql") String sql) {
//        Dataset<Row> clients = clientService.getAllClients(sql);
//        String json = clients.toJSON().collectAsList().toString();
//        return json;
//    }
//	@GetMapping("/clients/spark")
//    public void sparkSQL() {
//        sparkService.queryTable();
//    }

//	@GetMapping("/spark")
//    public void importCsv() throws Exception {
//	spark.createHbaseTable();
//
//    }

@PutMapping("/update")
	public Client updateClient(@RequestBody Client client) {
		try {
			clientService.addClient(client);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return client;
	}

}
