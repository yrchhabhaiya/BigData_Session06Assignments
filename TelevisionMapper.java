package assignment62;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class TelevisionMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
	
	public void map(Text key, IntWritable value, Context context) 
			throws IOException, InterruptedException {
		context.write(value, key);
	}
}
