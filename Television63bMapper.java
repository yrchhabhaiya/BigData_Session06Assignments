package assignment63;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Television63bMapper extends Mapper<LongWritable, Text, Text, StateSales> {
	Text outKey = new Text();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\t");
		outKey.set(lineArray[0]);
		context.write(outKey, new StateSales(lineArray[1], Integer.parseInt(lineArray[2])));
	}
}
