package assignment61;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TelevisionReducer2 extends Reducer<IntWritable, Text, Text, IntWritable>
{
	IntWritable outValue = new IntWritable();
	
	public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		for (Text value : values) {
			context.write(value, key);
		}
	}
}