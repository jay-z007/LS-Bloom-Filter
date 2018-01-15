package bloomFilterImpl;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	BloomFilter  filter = new BloomFilter(100, 4);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");
		//System.out.println("\nkey "+key);
		for(String token :tokens){
			//if(!filter.contains(token)){
				//filter.add(token);
				context.write(new Text(token), NullWritable.get());
			}
		}

}


