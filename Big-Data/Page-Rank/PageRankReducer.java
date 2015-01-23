/**
 * Reducer class to compute the page rank using the formula rank+=rank*beta.
 * Here the beta value is taken to be 0.8;
 */

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducer extends Reducer<Text, Text, Text, Text>  {

	private final Text keypr= new Text();
	private Text valueout = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		
		
		ArrayList<String> inlinks = new ArrayList<String>();
		ArrayList<String> outlinks = new ArrayList<String>();
		ArrayList<String> ranks = new ArrayList<String>();
		for(Text t:values)
		{
			String s = t.toString();
			if(s.contains(","))
			{
				String r[] = s.split(",");
				inlinks.add(r[0]);
				ranks.add(r[1]);
			}
			else
			{
				outlinks.add(s);
			}
		}
		Double rank=0.0;
		// compute the rank using beta value
		for(String inrank:ranks)
		{
			Double thisrank = Double.parseDouble(inrank);
			rank = rank + thisrank*0.8;
		}
		String outValue="";
		String kval = key.toString();
		String keyValue= kval+"\t"+rank;
		int i =0;
		for(String outl:outlinks)
		{
			
			if(i==outlinks.size()-1)
			{
				outValue+= outl;
			}
			else
			{
				outValue+= outl + ",";
			}
			
			i++;
		}
		keypr.set(keyValue);
		valueout.set(outValue);
		context.write(keypr, valueout);
		// set the key value pair in such a manner that it is like the input so that the iteration is easier.
		}
}
