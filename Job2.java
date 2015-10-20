
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class Job2 {
	

	public static class Map extends TableMapper<Text, Text>{
		
		private Text key1 = new Text();
		private Text value1 = new Text();
		
		
		@Override
		public void map(ImmutableBytesWritable rowkey, Result value, Context context) throws IOException, InterruptedException {
			
			

			String companyKey = Bytes.toString(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")));
			String year = Bytes.toString(value.getValue(Bytes.toBytes("time"), Bytes.toBytes("yr")));
			String month = Bytes.toString(value.getValue(Bytes.toBytes("time"), Bytes.toBytes("mm")));
			String day = Bytes.toString(value.getValue(Bytes.toBytes("time"), Bytes.toBytes("dd")));
			String adjClosePr = Bytes.toString(value.getValue(Bytes.toBytes("price"), Bytes.toBytes("price")));
			
			
			key1.set(companyKey+","+year+","+month);
			value1.set(day+","+adjClosePr);
			context.write(key1, value1);
		}
		
		
	}
	

	public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable>{
		
		int count = 1;
		
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
			
			HashMap<Integer, Double> map = new HashMap<Integer, Double>();
			int firstDay=32;
			int lastDay=0;
			double xi=0;
			
			for(Text value:values){
				String line = value.toString();
				String day[]=line.split(",");
				map.put(Integer.parseInt(day[0]),Double.parseDouble(day[1]));
			}
			
			
			for(int day:map.keySet()){
				if(lastDay<day)
			     lastDay=day; 
			}
			  
			for(int day:map.keySet()){
				if(firstDay>day)
			     firstDay=day; 
			}
			xi=(map.get(lastDay)-map.get(firstDay))/map.get(firstDay);
			Text dummy = new Text(xi+"");
			byte[] rowid = Bytes.toBytes(key.toString()+ count++);
			Put p = new Put(rowid);
			String key2=key.toString();
			String file[]=key2.split(",");
			p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), Bytes.toBytes(file[0]));
			p.add(Bytes.toBytes("volatility"), Bytes.toBytes("volatility"),Bytes.toBytes(dummy.toString()));
			context.write(new ImmutableBytesWritable(rowid), p);
			//System.out.println(p);
			
		}
		
	}
}
