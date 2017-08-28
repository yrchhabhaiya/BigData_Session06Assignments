package assignment62;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TelevisionPartitioner extends Partitioner<IntWritable, Text> {

	@Override
	public int getPartition(IntWritable key, Text value, int numPartitions) {
			int sales = key.get();
			int partitionNumber;
			if (sales >= 0 && sales < 5) {
				partitionNumber = 0;
			}
			else {
				partitionNumber = 1;
			}
			return partitionNumber;
	}
}