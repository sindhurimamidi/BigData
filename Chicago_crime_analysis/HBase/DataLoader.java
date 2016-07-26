package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileReader;

public class DataLoader {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);


        // Create tables for crimes
        HTableDescriptor crimesTableDescriptor = new HTableDescriptor(TableName.valueOf("crimes"));

        crimesTableDescriptor.addFamily(new HColumnDescriptor("crimesData"));

        admin.createTable(crimesTableDescriptor);

        // Get a handle to the tables and read data into them from files
        HTable crimesTable = new HTable(conf, "crimes");

        File file = new File("/Users/Sattya/Documents/Sattya_MS/Big_Data/Project/DataSets/crimes.csv");

        BufferedReader bufferReader = new BufferedReader(new FileReader(file));
        String line;

        while ((line = bufferReader.readLine()) != null)   {
            String[] token= line.split(",");
            Put crime = new Put(Bytes.toBytes(token[0]));

          //  crime.add(Bytes.toBytes("crimesData"), Bytes.toBytes("caseNumber"), Bytes.toBytes(token[1]));
	    //    crime.add(Bytes.toBytes("crimesData"), Bytes.toBytes("date"), Bytes.toBytes(token[2]));
            crime.add(Bytes.toBytes("crimesData"), Bytes.toBytes("primaryType"), Bytes.toBytes(token[5]));
         //   crime.add(Bytes.toBytes("crimesData"), Bytes.toBytes("description"), Bytes.toBytes(token[6]));
            crime.add(Bytes.toBytes("crimesData"), Bytes.toBytes("year"), Bytes.toBytes(token[17]));
            crimesTable.put(crime);
        }
        bufferReader.close();
        crimesTable.flushCommits();
        crimesTable.close();
        System.out.println("Table created");
    }
}
