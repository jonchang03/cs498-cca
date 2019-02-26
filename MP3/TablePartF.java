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
import org.apache.hadoop.hbase.client.Get; // was missing

import org.apache.hadoop.hbase.util.Bytes;

public class TablePartF{

   public static void main(String[] args) throws IOException {

	// TODO      
	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER      

	/**
	SELECT p.name., p.power, p1.name, p1.power, p.color
	FROM powers p
	INNER JOIN powers p1
		ON p.color = p1.color
		AND p.name <> p1.name;
	 */
	
	// LOOP THROUGH 2 COPIES OF TABLE
	// FIND ALL LINES WHERE COLOR IS THE SAME BUT NAME IS DIFFERENT

	// Instantiating Configuration class
	Configuration config = HBaseConfiguration.create();
	// Instantiating HTable class
	HTable table = new HTable(config, "powers");

	// Scanning the required columns
	// scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
	// scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"));
	// scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));
	// scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("xp"));
	// scan.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"));

	// Outer loop: Instantiating the Scan class + Getting the scan result
	ResultScanner scanner = table.getScanner(new Scan());
	for (Result result = scanner.next(); result != null; result = scanner.next()) {
		// Reading values from Result class object
		Result row = table.get(new Get(result.getRow()));
		String p_hero = Bytes.toString(row.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero")));
		String p_power = Bytes.toString(row.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power")));
		String p_name = Bytes.toString(row.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name")));
		String p_xp = Bytes.toString(row.getValue(Bytes.toBytes("professional"),Bytes.toBytes("xp")));
		String p_color = Bytes.toString(row.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color")));

		// Inner loop: Instantiating the Scan class + Getting the scan result
		ResultScanner scanner1 = table.getScanner(new Scan());
		for (Result result1 = scanner1.next(); result1 != null; result1 = scanner1.next()) {


			Result row1 = table.get(new Get(result1.getRow()));
			String p1_hero = Bytes.toString(row1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("hero")));
			String p1_power = Bytes.toString(row1.getValue(Bytes.toBytes("personal"),Bytes.toBytes("power")));
			String p1_name = Bytes.toString(row1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("name")));
			String p1_xp = Bytes.toString(row1.getValue(Bytes.toBytes("professional"),Bytes.toBytes("xp")));
			String p1_color = Bytes.toString(row1.getValue(Bytes.toBytes("custom"),Bytes.toBytes("color")));

			if (p_color.equals(p1_color) && !(p_name.equals(p1_name))) {
				String name = p_name;
				String power = p_power;
				String color = p_color;

				String name1 = p1_name;
				String power1 = p1_power;
				String color1 = p1_color;
				System.out.println(name + ", " + power + ", " + name1 + ", " + power1 + ", "+color);
			}
			// System.out.println(name + ", " + power + ", " + name1 + ", " + power1 + ", "+color);
			// System.out.println(p_name + ", " + p_power + ", " + p1_name + ", " + p1_power + ", "+p_color);

		}	
		scanner1.close();
	}
	//closing the scanner
	scanner.close();
   }
}
