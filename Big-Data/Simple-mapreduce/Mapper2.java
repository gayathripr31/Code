/*
 * This mapper passes the brand pairs to the reducer
 * 
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text brandPairs = new Text();
    
	public void map(LongWritable key, Text value, Context con)	throws IOException, InterruptedException {
		
		String pairStr = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(pairStr);
		while (tokenizer.hasMoreTokens()) {
			String brandStr = tokenizer.nextToken();
			//to check for the last record
			if(!brandStr.contains("("))
				continue;
			brandPairs.set(brandStr);
			
		//Pass the brandpair value to the reducer
			con.write(brandPairs, one);
		
		}
	}
}
