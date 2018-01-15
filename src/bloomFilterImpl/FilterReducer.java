package bloomFilterImpl;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FilterReducer extends Reducer<Text, Text, Text, Text> {
	
	BloomFilter filter = new BloomFilter(100, 3);
	
	
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		/*for (Text val : values) {
			
		}*/
		Boolean temp = filter.contains(_key);
		if(temp)
			System.out.println(_key+" already contains");
		filter.add(_key);

		System.out.println("\n"+_key + " " + temp);
		
		context.write(new Text(_key), new Text());
	
	}
	
}
