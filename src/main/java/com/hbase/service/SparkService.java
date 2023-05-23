//package com.hbase.service;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
//import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.springframework.stereotype.Service;
//
//@Service
//public class SparkService {
//
//
//	public void queryTable(){
//		SparkSession spark = SparkSession
//                .builder()
//                .appName("HBase Spark Query")
//                .config("spark.master", "local[*]")
//                .config("spark.sql.catalogImplementation", "in-memory")
//                .config("spark.hbase.host", "hadoop-master")
//                .config("spark.hbase.zookeeper.quorum", "hadoop-slave-1:2181")
//                .getOrCreate();
//
//
//		String catalog ="{\r\n"
//				+ "    \"table\":{\"namespace\":\"default\", \"name\":\"client\"},\r\n"
//				+ "    \"rowkey\":\"key\",\r\n"
//				+ "    \"columns\":{\r\n"
//				+ "        \"col0\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"},\r\n"
//				+ "        \"col1\":{\"cf\":\"cf1\", \"col\":\"col1\", \"type\":\"boolean\"},\r\n"
//				+ "        \"col2\":{\"cf\":\"cf2\", \"col\":\"col2\", \"type\":\"double\"},\r\n"
//				+ "        \"col3\":{\"cf\":\"cf3\", \"col\":\"col3\", \"type\":\"float\"},\r\n"
//				+ "        \"col4\":{\"cf\":\"cf4\", \"col\":\"col4\", \"type\":\"int\"},\r\n"
//				+ "        \"col5\":{\"cf\":\"cf5\", \"col\":\"col5\", \"type\":\"bigint\"},\r\n"
//				+ "        \"col6\":{\"cf\":\"cf6\", \"col\":\"col6\", \"type\":\"smallint\"},\r\n"
//				+ "        \"col7\":{\"cf\":\"cf7\", \"col\":\"col7\", \"type\":\"string\"}\r\n"
//				+ "    }\r\n"
//				+ "}";
//
//
//
////        Dataset<Row> hbaseDF = spark.read()
////                .option(HBaseTableCatalog.tableCatalog(), catalog)
////                .format("org.apache.spark.sql.execution.datasources.hbase")
////                .load();
//
//        Dataset<Row> hbaseDF = spark.read().format("org.apache.hadoop.hbase.spark")
//	            .option("hbase.table", "client")
//	            .option("hbase.mapreduce.inputtable", "client")
//	            .option("hbase.mapreduce.scan.row.start", "1")
//	            .option("hbase.mapreduce.scan.row.stop", "100000")
//	            .load();
//
//        Dataset<Row> result = spark.sql("SELECT * FROM client");
//        hbaseDF.show();
//
//        result.show();
//
//
//        spark.close();
//	}
//	
//	 
//
//	}
//
//
