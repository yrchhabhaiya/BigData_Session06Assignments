package assignment62;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @author Yogesh
 * Television.Java
 * Purpose:  Enhance the Map Reduce program of Task 8 (refer session 6, assignment 1) to use multiple reducers for sorting.
 * The driver should accept three additional values: the minimum units sold, the maximum units sold and number of reducers to use. 
 * Use units sold as key and company as value. 
 * Write a custom partitioner to divide the keys on the basis of range. Take minimum to be 0 and maximum to be 10. Divide them across 2 reducers.
 * 
 * @param String parameter as hdfs path of input file (output file of assignment61) & hdfs output directory.
 * 
 */

public class Television {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Assignment62");
		job.setJarByClass(Television.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(TelevisionMapper.class);
		job.setReducerClass(TelevisionReducer.class);
		job.setPartitionerClass(TelevisionPartitioner.class);
		
		//Not using Combiner here, as it produces output of different type as compared to the output of map()
		//job.setCombinerClass(Task9Reducer.class);
		
		job.setNumReduceTasks(2);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		job.waitForCompletion(true);
	}
}
