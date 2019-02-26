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
import org.apache.hadoop.hbase.client.Get;


import org.apache.hadoop.hbase.util.Bytes;

public class TablePartD{

   public static void main(String[] args) throws IOException {

	// TODO      
	// DON'T CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER 
	
	// Instantiating Configuration class
	Configuration config = HBaseConfiguration.create();

	// Instantiating HTable class
	HTable table = new HTable(config, "powers");

	// BEGIN: Id: "row1", Values for (hero, power, name, xp, color)
	// Instantiating Get class
	Get g = new Get(Bytes.toBytes("row1"));

	// Reading the data
	Result result = table.get(g);
	
	// Reading values from Result class object
	byte [] v1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	byte [] v2 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power"));
	byte [] v3 = result.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
	byte [] v4 = result.getValue(Bytes.toBytes("professional"),Bytes.toBytes("xp"));
	byte [] v5 = result.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));

	String hero = Bytes.toString(v1);
	String power = Bytes.toString(v2);
	String name = Bytes.toString(v3);
	String xp = Bytes.toString(v4);
	String color = Bytes.toString(v5);
	System.out.println("hero: "+hero+", power: "+power+", name: "+name+", xp: "+xp+", color: "+color);
	// END: Id: "row1", Values for (hero, power, name, xp, color)


	// BEGIN: Id: "row19", Values for (hero, color)
	g = new Get(Bytes.toBytes("row19"));
	result = table.get(g);
	v1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	v5 = result.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));

	hero = Bytes.toString(v1);
	color = Bytes.toString(v5);
	System.out.println("hero: "+hero+", color: "+color);
	// END: Id: "row19", Values for (hero, color)



	// BEGIN: Id: "row1", Values for (hero, name, color)
	g = new Get(Bytes.toBytes("row1"));
	result = table.get(g);
	v1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero"));
	v3 = result.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name"));
	v5 = result.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color"));

	hero = Bytes.toString(v1);
	name = Bytes.toString(v3);
	color = Bytes.toString(v5);
	System.out.println("hero: "+hero+", name: "+name+", color: "+color); 
	// END: Id: "row1", Values for (hero, name, color)
   }
}

