/*
 * This mapper class concatenates the timestamp and brand
 * and passes the userid as key and timebrand as value to the partitioner class
 */


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


public class Mapper1 extends Mapper <LongWritable, Text, Text, Text> {
	
	
    //Hadoop Text variables for userID and timebrand value
	private final static Text userKey = new Text();
	private Text btime_values = new Text();

	public void map(LongWritable key, Text value,
			Context con) throws IOException, InterruptedException {
		//The Text value is converted to String for processesing
		String record = value.toString();
		//Tokenizer splits the values in the record
		StringTokenizer token = new StringTokenizer(record,",");
		
		while(token.hasMoreTokens())
		{
			//String values are assigned for all the respective parts
			String user = token.nextToken();
			String bname = token.nextToken();
			String tstamp = token.nextToken();
			
			//The String userid is converted to Text type
		    userKey.set(user);
		    //Time and brand value concatenated
		    String btime =  tstamp + "->" + bname;
		    //The btime value set to Text type
		    btime_values.set(btime);
		    //pass the key value to the reducer
		    con.write(userKey, btime_values);			
		}
		
	}
	



	

	

}
