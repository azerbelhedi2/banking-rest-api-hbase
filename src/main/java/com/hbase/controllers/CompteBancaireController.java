package com.hbase.controllers;

import java.io.IOException;
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
import org.springframework.web.bind.annotation.RestController;

import com.hbase.schemas.CompteBancaire;
import com.hbase.schemas.Solde;
import com.hbase.service.ICompteBancaireService;

@RestController
@RequestMapping("/api/comptes")
public class CompteBancaireController {


	@Autowired
	ICompteBancaireService compteBancaireService;
	@PostMapping("/add")
	public CompteBancaire addCompteBancaire(@RequestBody CompteBancaire compte) {
		try {
			compteBancaireService.createCompteBancaire(compte);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return compte;
	}
	@DeleteMapping("/delete/{id}")
	public void deletCompteBancaire(@PathVariable("id") String id) {
		try {
			compteBancaireService.deleteCompteBancaire(id);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@GetMapping("/get/{id}")
	public CompteBancaire getById(@PathVariable("id") String id) {
		try {
			return compteBancaireService.getByIdCb(id);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	@GetMapping("/query")
	public List<CompteBancaire> queryAllComptes() {
		try {
			return compteBancaireService.findAll();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;


	}
	@GetMapping("/query/{idClient}")
	public List<CompteBancaire> queryClientComptes(@PathVariable String idClient) {
		try {
			return compteBancaireService.findAllCompteByIdClient(idClient);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;


	}
	@GetMapping("/historiqueSolde/{rowKey}")
	public List<Solde> getHistoriqueSolde(@PathVariable String rowKey){
		try {
			return compteBancaireService.getHistoriqueSolde(rowKey);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 
			return null;
		}
	}
	@PutMapping("/update")
	public CompteBancaire udpateCompteBancaire(@RequestBody CompteBancaire compte) {
		try {
			compteBancaireService.createCompteBancaire(compte);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return compte;
	}
	
	@PutMapping("/batchUpdateSolde")
	public String batchUpdateSolde(@RequestBody List<CompteBancaire> listComptes) {
		try {
			compteBancaireService.batchUpdateCompteBancaireSolde(listComptes);
			return "UPDATE SUCCESS";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "UPDATE FAILED";
		}
	}

}
