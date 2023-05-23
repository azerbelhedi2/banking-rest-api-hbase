package com.hbase.schemas;

public class HBaseStatus {
	private String hbase_version ;
	private String hbase_cluster_id;
	private String master ;
	private String backup_master;
	private  String live_region;
	private int dead_region ;
	private double average_load;
	private int server_size;

	public HBaseStatus() {
		super();
		// TODO Auto-generated constructor stub
	}
	public HBaseStatus(String hbase_version, String hbase_cluster_id, String master, String backup_master,
			String live_region, int dead_region, double average_load, int server_size) {
		super();
		this.hbase_version = hbase_version;
		this.hbase_cluster_id = hbase_cluster_id;
		this.master = master;
		this.backup_master = backup_master;
		this.live_region = live_region;
		this.dead_region = dead_region;
		this.average_load = average_load;
		this.server_size = server_size;
	}
	public double getAverage_load() {
		return average_load;
	}
	public String getBackup_master() {
		return backup_master;
	}
	public int getDead_region() {
		return dead_region;
	}
	public String getHbase_cluster_id() {
		return hbase_cluster_id;
	}
	public String getHbase_version() {
		return hbase_version;
	}
	public String getLive_region() {
		return live_region;
	}
	public String getMaster() {
		return master;
	}
	public int getServer_size() {
		return server_size;
	}

	public void setAverage_load(double average_load) {
		this.average_load = average_load;
	}
	public void setBackup_master(String backup_master) {
		this.backup_master = backup_master;
	}
	public void setDead_region(int dead_region) {
		this.dead_region = dead_region;
	}
	public void setHbase_cluster_id(String hbase_cluster_id) {
		this.hbase_cluster_id = hbase_cluster_id;
	}
	public void setHbase_version(String hbase_version) {
		this.hbase_version = hbase_version;
	}
	public void setLive_region(String live_region) {
		this.live_region = live_region;
	}
	public void setMaster(String master) {
		this.master = master;
	}
	public void setServer_size(int server_size) {
		this.server_size = server_size;
	}





}
