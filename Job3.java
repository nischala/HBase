
	
		import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

		import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class Job3 {

	public static class Map extends TableMapper<Text,Text>{
		private Text key2 = new Text();
		private Text value2 = new Text();
		public void map(ImmutableBytesWritable rowkey, Result value, Context context) throws IOException, InterruptedException {
			String file = Bytes.toString(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")));
			String xi = Bytes.toString(value.getValue(Bytes.toBytes("volatility"), Bytes.toBytes("volatility")));
					
			key2.set(file);
			value2.set(xi);
			context.write(key2,value2);
					
		}
	}
			
	public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable>{
				
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {	
					
					List<Double> list = new ArrayList<Double>();
					int N=0;
					double mean=0;
					double sum=0;
					double volatility=0;
					
					
					for(Text value:values) {
				    Double a=Double.parseDouble(value.toString());
				    list.add(a);
				    N++;
					}
					for(int i=0;i<N;i++) {
						mean=(mean+list.get(i)/N);
					}
					for(int j=0;j<N;j++) {
						double diff=list.get(j)-mean;
						double sqr=Math.pow(diff, 2);
						sum=sum+sqr;
					}
					volatility=Math.sqrt(sum/(N-1));
					byte[] rowid = Bytes.toBytes(key.toString());
					Text dummy = new Text(volatility+"");
					Put p = new Put(rowid);
					if(volatility>0) {
					p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), Bytes.toBytes(key.toString()));
					p.add(Bytes.toBytes("volatility"), Bytes.toBytes("volatility"),Bytes.toBytes(dummy.toString()));
					context.write(null, p);
					}
				}
				
				
		}
				
				
}
		




