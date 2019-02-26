import java.io.IOException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.util.*;

public class TablePartC{

   public static void main(String[] args) throws IOException {
	   //TODO
      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();
      // Instantiating HTable class
      HTable hTable = new HTable(config, "powers");

      // populate HBase Table with data from csv
      Scanner scanner = new Scanner(new File("input.csv"));
      
      // declare the schema
      String[] columnFamilies = {"personal", "professional", "custom"};
      String[] columnNames = {"hero", "power", "name", "xp", "color"};

      while(scanner.hasNextLine()) {
         String[] line = scanner.nextLine().split(",");
         // Instantiating Put class
         // accepts a row name.
         Put p = new Put(Bytes.toBytes(line[0]));

         // adding values using add() method
         // accepts column family name, qualifier/row name ,value
         p.add(Bytes.toBytes(columnFamilies[0]), 
               Bytes.toBytes(columnNames[0]), Bytes.toBytes(line[1]));

         p.add(Bytes.toBytes(columnFamilies[0]), 
               Bytes.toBytes(columnNames[1]), Bytes.toBytes(line[2]));

         p.add(Bytes.toBytes(columnFamilies[1]), 
               Bytes.toBytes(columnNames[2]), Bytes.toBytes(line[3]));

         p.add(Bytes.toBytes(columnFamilies[1]), 
               Bytes.toBytes(columnNames[3]), Bytes.toBytes(line[4]));

         p.add(Bytes.toBytes(columnFamilies[2]), 
               Bytes.toBytes(columnNames[4]), Bytes.toBytes(line[5]));
         hTable.put(p);
      }
      // Saving the put Instance to the HTable.
      System.out.println("data inserted");
      // closing HTable
      hTable.close();

         
   }
}

