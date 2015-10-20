
import java.util.Date;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Main{


	public static void main(String[] args){

		Configuration conf = HBaseConfiguration.create();
		try {
			long start = new Date().getTime();
			HBaseAdmin admin = new HBaseAdmin(conf);
			
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("Data"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("time"));
			tableDescriptor.addFamily(new HColumnDescriptor("price"));
			if ( admin.isTableAvailable("Data")){
				admin.disableTable("Data");
				admin.deleteTable("Data");
			}
			admin.createTable(tableDescriptor);

			
			tableDescriptor = new HTableDescriptor(TableName.valueOf("xi_values"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("volatility"));
			
			if ( admin.isTableAvailable("xi_values")){
				admin.disableTable("xi_values");
				admin.deleteTable("xi_values");
			}
			admin.createTable(tableDescriptor);

			
			tableDescriptor = new HTableDescriptor(TableName.valueOf("stock_volatility"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("volatility"));
			
			if ( admin.isTableAvailable("stock_volatility")){
				admin.disableTable("stock_volatility");
				admin.deleteTable("stock_volatility");
			}
			admin.createTable(tableDescriptor);
			
			tableDescriptor = new HTableDescriptor(TableName.valueOf("final"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("volatility"));
			
			if ( admin.isTableAvailable("final")){
				admin.disableTable("final");
				admin.deleteTable("final");
			}
			admin.createTable(tableDescriptor);
			
			
			Job job = Job.getInstance();
			job.setJarByClass(Main.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob("Data", null, job);
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
			
			
			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(Main.class);
			Scan scan = new Scan();
			TableMapReduceUtil.initTableMapperJob("Data", scan, Job2.Map.class, Text.class, Text.class, job2);			
			TableMapReduceUtil.initTableReducerJob("xi_values", Job2.Reduce.class, job2);
			job2.waitForCompletion( true );
			
			
			Job job3 = Job.getInstance(conf);
			job3.setJarByClass(Main.class);
			Scan scan3 = new Scan();
			TableMapReduceUtil.initTableMapperJob("xi_values", scan3, Job3.Map.class, Text.class, Text.class, job3);
			TableMapReduceUtil.initTableReducerJob("stock_volatility", Job3.Reduce.class, job3);			
			job3.waitForCompletion(true);
			
			Job job4 = Job.getInstance(conf);
			job4.setJarByClass(Main.class);
			Scan scan4 = new Scan();
			TableMapReduceUtil.initTableMapperJob("stock_volatility", scan4, Job4.Map.class, Text.class, Text.class, job4);
			TableMapReduceUtil.initTableReducerJob("final", Job4.Reduce.class, job4);			
			boolean status =  job4.waitForCompletion(true);
			
			
			HTable table = new HTable(conf, "final");
			Scan topScan = new Scan();
			ResultScanner resultScan = table.getScanner(topScan);
			int count = 0;		
			TreeMap<Double, String> treemap = new TreeMap<Double, String>();
			for(Result value : resultScan)
			{
				treemap.put(Double.parseDouble(Bytes.toString(value.getValue(Bytes.toBytes("volatility"), Bytes.toBytes("volatility")))),
						Bytes.toString(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name"))));
			}
			
			System.out.println("\nThe top 10 stocks with the minimum volatility are:\n");
			
			for(Double i:treemap.keySet()){
				
				if(count < 10) {
					System.out.println(treemap.get(i) + "  " + i);
					count++;
				}
				else
					break;
				
				
			}
			System.out.println("\nThe top 10 stocks with the maximum volatility are:\n");
			count = 0;
			for(Double i:treemap.descendingKeySet()){
				
				if(count < 10) {
					System.out.println(treemap.get(i) + "  " + i);
					count++;
				}
				
				else
					break;
				
			}
			
			if (status == true) {
				long end = new Date().getTime();
				System.out.println("\nThe time taken by job is " + (end-start)/1000 + " Seconds\n");
			}
			table.close();
			admin.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}



