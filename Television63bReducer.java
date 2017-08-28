package assignment63;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Television63bReducer extends Reducer<Text, StateSales, Text, StateSales>
{	
	ArrayList<StateSales> stateSalesData = new ArrayList<StateSales>();
	
	public void reduce(Text key, Iterable<StateSales> values,Context context) throws IOException, InterruptedException
	{
		stateSalesData.clear();
		
		for (StateSales value : values) {
			//value is a single object containing different instances in different passes.
			//So, create new StateSales objects every time we need to store it to the ArrayList
			stateSalesData.add(new StateSales(value.getState(), value.getSales()));
		}
		
		Collections.sort(stateSalesData);
		
		int counter = 1;
		for (StateSales statesales : stateSalesData) {
			if (counter > 3) {
				break;
			}
			context.write(key, statesales);
			counter++;
		}		
	}
}
