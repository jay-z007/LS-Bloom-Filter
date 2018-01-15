package bloomFilterImpl;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FilterDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobAdd");
		job.setJarByClass(bloomFilterImpl.FilterDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(FilterMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(FilterReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);


/*		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class,
				 Text.class, Text.class);
*/		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/home/rohit/workspace/Bhatt/input"));
		FileOutputFormat.setOutputPath(job, new Path("/home/rohit/workspace/Bhatt/output"));
		
		if (!job.waitForCompletion(true))
		{	
			return;
		}

/*		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "JobCheck");
		job1.setJarByClass(bloomFilterImpl.FilterDriver.class);
		// TODO: specify a mapper
		job1.setMapperClass(FilterMapper.class);
		// TODO: specify a reducer
		job1.setReducerClass(CheckingReducer.class);

		// TODO: specify output types
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);

		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job1, new Path("/home/jbhatt/input1"));
		FileOutputFormat.setOutputPath(job1, new Path("/home/jbhatt/output1"));
		
		if (!job1.waitForCompletion(true))
		{	
			return;
		}
*/

	
	}
}
