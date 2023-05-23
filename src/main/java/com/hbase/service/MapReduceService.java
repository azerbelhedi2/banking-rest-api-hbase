package com.hbase.service;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TsvImporterMapper;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;



@Service
public class MapReduceService {

    private static final String HADOOP_FILE_PATH = "hdfs://hadoop-master:9000/files/";
	private static final String TABLE_NAME = "client";
    private static final String COLUMNS = "HBASE_ROW_KEY,info_client:firstname,info_client:lastname,info_client:cin,info_client:adresse,info_client:email,info_client:tel,info_client:date_inscription,info_client:date_naissance";
    private static final String SEPARATOR = ";";
    private static final Log LOGGER = LogFactory.getLog(HBaseService.class);

    	@Autowired
    	Configuration conf;

    public void insertWithMapReduce(String filePath) throws Exception {
         conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "ImportTsv");
        job.setJarByClass(TsvImporterMapper.class);
        job.getConfiguration().set(ImportTsv.BULK_OUTPUT_CONF_KEY, TABLE_NAME);
        job.getConfiguration().set(ImportTsv.COLUMNS_CONF_KEY, COLUMNS);
        job.getConfiguration().set(ImportTsv.SEPARATOR_CONF_KEY, SEPARATOR);
        job.setMapperClass(TsvImporterMapper.class);
        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, null, job);
        job.setNumReduceTasks(0);
        job.getConfiguration().set("-Dimporttsv.separator", ";");
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, new Path(filePath));
        job.waitForCompletion(true);
    }
    public void importCSVToHBase() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("importtsv.separator", ";");
        config.set("importtsv.columns", "HBASE_ROW_KEY,compte_bancaire:idClient,compte_bancaire:numero_compte,compte_bancaire:date_creation,compte_bancaire:solde,compte_bancaire:type_compte");
        String[] args = { "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,compte_bancaire:idClient,compte_bancaire:numero_compte,compte_bancaire:date_creation,compte_bancaire:solde,compte_bancaire:type_compte"
        		 ,"-D" + ImportTsv.SEPARATOR_CONF_KEY + "=;","database_client", "hdfs://hadoop-master:9000/files/comptebancaires_clients_database.csv"};
        int status = ToolRunner.run(config, new ImportTsv(), args);
        if (status == 0) {
            System.out.println("Import completed successfully.");
        } else {
            System.out.println("Import failed with status: " + status);
        }
    }
    public void importCSVToHBaseTable(MultipartFile file , String tableName , String columnFamily) throws Exception {
        Configuration config = HBaseConfiguration.create();
        String columns = "HBASE_ROW_KEY,"+columnFamily+":idClient,"+columnFamily+":numero_compte,"+columnFamily+":date_creation,"+columnFamily+":solde,"+columnFamily+":type_compte";
        config.set("importtsv.separator", ";");
        config.set("importtsv.columns", columns);
        String[] args = { "-D" + ImportTsv.COLUMNS_CONF_KEY + "="+columns
        		 ,"-D" + ImportTsv.SEPARATOR_CONF_KEY + "=;",tableName, "hdfs://hadoop-master:9000/files/"+file.getOriginalFilename()};
        int status = ToolRunner.run(config, new ImportTsv(), args);
        if (status == 0) {
            System.out.println("Import completed successfully.");
        } else {
            System.out.println("Import failed with status: " + status);
        }
    }
    public void importCSVToHBaseTableWithHeader(MultipartFile file , String tableName , String columnFamily) throws Exception {
        Configuration config = HBaseConfiguration.create();
        String columns = "HBASE_ROW_KEY,";
        addFileToHadoop(file);
        String[] headers = getCSVHeader(file);
        if (headers == null) {
            throw new IllegalArgumentException("CSV header is missing.");
        }

        for (String header : headers) {
            columns = columns + columnFamily + ":" + header + ",";
        }
        for ( String header : headers) {
        	columns=columns+columnFamily+":"+header+",";
        }
        config.set("importtsv.separator", ";");
        config.set("importtsv.columns", columns);
        String[] args = { "-D" + ImportTsv.COLUMNS_CONF_KEY + "="+columns
        		 ,"-D" + ImportTsv.SEPARATOR_CONF_KEY + "=;",tableName, "hdfs://hadoop-master:9000/files/"+file.getOriginalFilename()};
        int status = ToolRunner.run(config, new ImportTsv(), args);
        if (status == 0) {
            System.out.println("Import completed successfully.");
        } else {
            System.out.println("Import failed with status: " + status);
        }
    }
    
    public String[] getCSVHeader(MultipartFile file) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
        String headerLine = reader.readLine();
        reader.close();

        if (StringUtils.hasText(headerLine)) {
            return headerLine.split(";");
        }

        return null; // Ou lancez une exception appropriée si le header est manquant
    }
    public void insertCsvMapReduce(String fileName) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Connection connection = null;
        Admin admin = null;
        Table table = null;
        String tableName = "client"; // Nom de la table dans HBase
        try {
          // Configuration de la connexion HBase
          config.set("hbase.zookeeper.quorum", "hadoop-slave-1"); // Adresse IP du serveur ZooKeeper
          config.set("hbase.zookeeper.property.clientPort", "2181"); // Port par défaut de ZooKeeper
          config.set("zookeeper.znode.parent", "/hbase");
          config.set("hbase.client.scanner.timeout.period", "600000"); // Durée d'expiration du scanner
          connection = ConnectionFactory.createConnection(config);
          admin = connection.getAdmin();
          
          // Vérification si la table existe
          if (!admin.tableExists(TableName.valueOf(tableName))) {
            LOGGER.error("La table " + tableName + " n'existe pas !");
            return;
          }
          
          // Configuration du job MapReduce
          Job job = Job.getInstance(config, "ImportTsv-" + UUID.randomUUID().toString());
          job.setJarByClass(HBaseService.class);
          job.setMapperClass(TsvImporterMapper.class);
          job.setMapOutputKeyClass(ImmutableBytesWritable.class);
          job.setMapOutputValueClass(Object[].class);
          TableMapReduceUtil.initTableReducerJob(tableName, null, job);
          job.getConfiguration().set(ImportTsv.COLUMNS_CONF_KEY, "HBASE_ROW_KEY,info_client:adresse,info_client:cin,info_client:date_inscription,info_client:date_naissance,info_client:email,info_client:firstname,info_client:lastname,info_client:tel");
          job.getConfiguration().set(ImportTsv.SEPARATOR_CONF_KEY, ";");
          job.getConfiguration().set(ImportTsv.SKIP_LINES_CONF_KEY, "1");
          job.getConfiguration().set(ImportTsv.BULK_OUTPUT_CONF_KEY, "/tmp/ImportTsv-" + UUID.randomUUID().toString());
          
          // Ajout du fichier CSV à l'entrée du job
          Path inputPath = new Path(fileName);
          FileSystem fs = FileSystem.get(inputPath.toUri(), config);
          job.addCacheFile(inputPath.toUri());
          fs.setPermission(inputPath, FsPermission.valueOf("-rw-r--r--"));
          if (job.waitForCompletion(true)) {
              LOGGER.info("Importation du fichier CSV " + fileName + " dans la table " + tableName + " effectuée avec succès !");
            } else {
              LOGGER.error("Erreur lors de l'importation du fichier CSV " + fileName + " dans la table " + tableName + " !");
            }
          } catch (Exception e) {
            LOGGER.error("Erreur lors de l'importation du fichier CSV " + fileName + " dans la table " + tableName + " : " + e.getMessage(), e);
          } finally {
            // Fermeture des ressources
            if (table != null) {
              table.close();
            }
            if (admin != null) {
              admin.close();
            }
            if (connection != null) {
              connection.close();
            }
          }
        }
    public void addFileToHadoop(MultipartFile csvFile) throws IOException, URISyntaxException {
         conf = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI("hdfs://hadoop-master:9000"),conf);
        String fileName = csvFile.getOriginalFilename();
        String hadoopFilePath = HADOOP_FILE_PATH + fileName;

        Path filePath = new Path(hadoopFilePath);
        if (hdfs.exists(filePath)) {
            System.out.println("Le fichier existe déjà sur Hadoop : " + hadoopFilePath);
            return;
        }
        try (InputStream inputStream = csvFile.getInputStream();
             OutputStream outputStream = hdfs.create(filePath)) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }

        System.out.println("Le fichier a été sauvegardé avec succès Sur hdfs://hadoop-master:9000/files : " + hadoopFilePath);
    }
}
