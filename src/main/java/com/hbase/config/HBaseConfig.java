package com.hbase.config;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class HBaseConfig {

	@Bean
	org.apache.hadoop.conf.Configuration configuration() {
		org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "hadoop-slave-1");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.rootdir", "hdfs://hadoop-master:9000/hbase");
		configuration.set("hbase.cluster.distributed", "true");
		String path = this.getClass()
				  .getClassLoader()
				  .getResource("hbase-site.xml")
				  .getPath();
		configuration.addResource(new Path(path));
		return configuration;
	}

	@Bean
	 Connection connection() throws IOException {
		Connection conn = ConnectionFactory.createConnection(configuration());
		return conn;

	}

//	@Bean
//	public SparkSession sparkSession() {
//		SparkSession sparkSession;
//		sparkSession = SparkSession.builder().appName("ClientService").master("local[*]")
//				.config("spark.hbase.host", "hadoop-master").config("spark.hbase.port", "2181")
//				.config("hbase.zookeeper.quorum", "hadoop-slave-1").getOrCreate();
//		try {
//			Connection conn = ConnectionFactory.createConnection(configuration());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return sparkSession;
//	}

}