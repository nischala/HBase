import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
public class Job4{
	
	public static class Map extends TableMapper<Text, Text>{
		
		public void map(ImmutableBytesWritable key, Result columns, Context context)throws IOException, InterruptedException {
			
			String volatility = Bytes.toString(columns.getValue(Bytes.toBytes("volatility"),Bytes.toBytes("volatility")));
			String file = Bytes.toString(columns.getValue(Bytes.toBytes("stock"),Bytes.toBytes("name")));
			context.write(new Text(" "),new Text(file+","+volatility));
					
		}
	}
	
	
public static class Reduce extends TableReducer< Text, Text, ImmutableBytesWritable> {
		
	static TreeMap<Double,Text> Map = new TreeMap<Double,Text>();
	 static int count=0;
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
			
			for(Text value:values){
	            String line=value.toString();
	            String[] stock_vol=line.split(",");
	            Map.put(Double.parseDouble(stock_vol[1]),new Text(stock_vol[0]));}
	             
	    		
			
			for(Double i:Map.keySet()){
				
				byte[] rowid = Bytes.toBytes(Map.get(i).toString());
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("stock"),Bytes.toBytes("name"),Bytes.toBytes(Map.get(i).toString()));
				p.add(Bytes.toBytes("volatility"),Bytes.toBytes("volatility"),Bytes.toBytes(String.valueOf(i)));
				context.write(new ImmutableBytesWritable(rowid),p);
				count++;
			    if(count>=10)
			    	break; 
				}
			
			count=0;
			for(Double i:Map.descendingKeySet()){
				
				byte[] rowid = Bytes.toBytes(Map.get(i).toString());
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("stock"),Bytes.toBytes("name"),Bytes.toBytes(Map.get(i).toString()));
				p.add(Bytes.toBytes("volatility"),Bytes.toBytes("volatility"),Bytes.toBytes(String.valueOf(i)));
				context.write(new ImmutableBytesWritable(rowid),p);
				count++;
			    if(count>=10)
			    	break; 
				
			  }
		}
	}
}

	
		