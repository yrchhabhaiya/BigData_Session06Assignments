package assignment61;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Yogesh
 * TelevisionMapper.Java
 * Purpose: Map company wise sale of televisions
 * 
 * MapOutput Key: State of type Text
 * MapOutput Values: List of company name of type Text.
 */

public class TelevisionMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	Text companyName;
	Text productName;
	public static final Text NA = new Text("NA");
	
	public void setup(Context context){
		companyName = new Text();
		productName = new Text();
	}
	
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
		String[] lineArray = values.toString().split("\\|");
		
		companyName.set(lineArray[0]);
		productName.set(lineArray[1]);
		
			context.write(companyName, new IntWritable(1));

		
	}
}
