package com.hbase.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hbase.schemas.Client;

public class ClientMapper extends TableMapper<ImmutableBytesWritable, Result> {
	private static final String TABLE_NAME = "client";
    private static final String FAMILY_INFO_CLIENTS = "info_client";
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Client client = new Client();
        client.setIdClient(Bytes.toString(key.get()));
        client.setLastname(Bytes.toString(value.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("lastname"))));
        client.setFirstname(Bytes.toString(value.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("firstname"))));
        client.setAdresse(Bytes.toString(value.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("adresse"))));
        client.setDate_inscription(Bytes.toString(value.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_inscription"))));
        client.setDate_naissance(Bytes.toString(value.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("date_naissance"))));
        client.setCin(Bytes.toString(value.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("cin"))));
        client.setTel(Bytes.toString(value.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("tel"))));
        client.setEmail(Bytes.toString(value.getValue(Bytes.toBytes(FAMILY_INFO_CLIENTS), Bytes.toBytes("email"))));

        // Émettre le client en tant que clé et valeur intermédiaire
        context.write(new ImmutableBytesWritable(Bytes.toBytes(client.getIdClient())), value);
    }
}