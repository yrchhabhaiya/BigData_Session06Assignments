package assignment63;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Television63aMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text outKey = new Text();
	IntWritable outValue = new IntWritable();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		outKey.set(lineArray[0] + "\t" + lineArray[3]);
		outValue.set(1);
		context.write(outKey, outValue);
	}
}
