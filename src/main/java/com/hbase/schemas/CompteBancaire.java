package com.hbase.schemas;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

public class CompteBancaire {

	private String idCb;
	private String idClient;
	private String numero_compte;
	private String solde;
	private String type_compte;
	@JsonProperty(access = Access.READ_ONLY)
	private String date_creation = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date());
	public CompteBancaire() {
		super();
		// TODO Auto-generated constructor stub
	}
	public String getDate_creation() {
		return date_creation;
	}
	public String getIdCb() {
		return idCb;
	}
	public String getIdClient() {
		return idClient;
	}
	public String getNumero_compte() {
		return numero_compte;
	}
	public String getSolde() {
		return solde;
	}
	public String getType_compte() {
		return type_compte;
	}
	public void setDate_creation(String date_creation) {
		this.date_creation = date_creation;
	}

	public void setIdCb(String idCb) {
		this.idCb = idCb;
	}
	public void setIdClient(String idClient) {
		this.idClient = idClient;
	}
	public void setNumero_compte(String numero_compte) {
		this.numero_compte = numero_compte;
	}
	public void setSolde(String solde) {
		this.solde = solde;
	}
	public void setType_compte(String type_compte) {
		this.type_compte = type_compte;
	}

}
