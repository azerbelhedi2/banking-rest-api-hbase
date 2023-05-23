package com.hbase.schemas;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

public class Client {


	private String idClient ;
	private String firstname;
	private String lastname ;
	private String email ;
	private String adresse;
	private String date_naissance;
	@JsonProperty(access = Access.READ_ONLY)
	private String date_inscription = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date());
	private String tel ;
	private String cin;
	@JsonProperty(access = Access.READ_ONLY)
	private String columnFamily ="info_client" ;
	public Client() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Client(String idClient, String firstname, String lastname, String email, String adresse,
			String date_naissance, String date_inscription, String tel, String cin) {
		super();
		this.idClient = idClient;
		this.firstname = firstname;
		this.lastname = lastname;
		this.email = email;
		this.adresse = adresse;
		this.date_naissance = date_naissance;
		this.date_inscription = date_inscription;
		this.tel = tel;
		this.cin = cin;
	}
	public String getAdresse() {
		return adresse;
	}
	public String getCin() {
		return cin;
	}
	public String getColumnFamily() {
		return columnFamily;
	}
	public String getDate_inscription() {
		return date_inscription;
	}
	public String getDate_naissance() {
		return date_naissance;
	}
	public String getEmail() {
		return email;
	}
	public String getFirstname() {
		return firstname;
	}
	public String getIdClient() {
		return idClient;
	}
	public String getLastname() {
		return lastname;
	}
	public String getTel() {
		return tel;
	}
	public void setAdresse(String adresse) {
		this.adresse = adresse;
	}
	public void setCin(String cin) {
		this.cin = cin;
	}
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}
	public void setDate_inscription(String date_inscription) {
		this.date_inscription = date_inscription;
	}
	public void setDate_naissance(String date_naissance) {
		this.date_naissance = date_naissance;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}
	public void setIdClient(String idClient) {
		this.idClient = idClient;
	}
	public void setLastname(String lastname) {
		this.lastname = lastname;
	}
	public void setTel(String tel) {
		this.tel = tel;
	}


}
