/*
 * This reducer gets the count for each brand pairs
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context con)
			throws IOException, InterruptedException {
		//Variable to store the count value for each pair
		int count = 0;
		for (IntWritable val : values) {
		    // get the count for each brand pair 
			count = count + val.get();
		}
		//Final output into the output file with the brand pairs and their count
		con.write(key, new IntWritable(count));
	}
}
