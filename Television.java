/**
 * Package contains Assignment 5 Task 1 from acadgild.
 */
package assignment61;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


/**
 * @author Yogesh
 * Television.Java
 * Purpose:  Write a Map Reduce program that takes the output of Task 5 (refer session 5, assignment 1) as input,
 * and produce output which is sorted on the total units sold.
 * You may use a single reducer for the sorting
 * 
 * @param String parameter as hdfs path of input file & hdfs output directory.
 * 
 */

public class Television {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		//get configuration
		Configuration conf = new Configuration();
		
		//instantiate Job(Configuration, JobName)
		Job job = new Job(conf, "Assignment61");
		
		//Jar class to initiate the program
		job.setJarByClass(Television.class);
		
		//set mapper/reducer classes
		job.setMapperClass(TelevisionMapper.class);
		job.setReducerClass(TelevisionReducer.class);
		
		//input ARGUMENTS to the program
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//input classes to Program
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//output classes of Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//output classes of Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Mapper Reducer Invocation
		job.waitForCompletion(true);
		
	}

}
