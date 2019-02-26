import java.io.IOException;

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

public class TablePartA{

   public static void main(String[] args) throws IOException {

	//TODO
   // Instantiating configuration class
   Configuration con = HBaseConfiguration.create();

   // Instantiating HbaseAdmin class
   HBaseAdmin admin = new HBaseAdmin(con);

   // Table 1
   // Instantiating table descriptor class
   HTableDescriptor tableDescriptor1 = new HTableDescriptor(TableName.valueOf("powers"));
   // Adding column families to table descriptor
   tableDescriptor1.addFamily(new HColumnDescriptor("personal"));
   tableDescriptor1.addFamily(new HColumnDescriptor("professional"));
   tableDescriptor1.addFamily(new HColumnDescriptor("custom"));

   // Execute the table through admin
   admin.createTable(tableDescriptor1);
   System.out.println(" Table 1 created ");

   // Table 2
   // Instantiating table descriptor class
   HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("food"));
   // Adding column families to table descriptor
   tableDescriptor2.addFamily(new HColumnDescriptor("nutrition"));
   tableDescriptor2.addFamily(new HColumnDescriptor("taste"));

   // Execute the table through admin
   admin.createTable(tableDescriptor2);
   System.out.println(" Table 2 created ");
   }
}

