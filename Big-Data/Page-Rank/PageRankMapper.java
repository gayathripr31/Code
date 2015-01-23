/**
 * Mapper class for page rank - prints the outlinks and their rank and node and their out links.
 */

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>  {
	
	private Text nodek=new Text();
	private Text outlk=new Text();
	private Text inrank=new Text();
	
	
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		String line=value.toString();
		String[] s=line.split("\t");
		String node=s[0];
		String rank=s[1];
		String outlinks=s[2];
		
		String[] outlinksm=null;
		if(outlinks.contains(",")){
			outlinksm=outlinks.split(",");
		}
		
		nodek.set(node);
		int degree;
		
		// if only one outlink is there we donot need to iterated the outlink list for printing them
		if(outlinks.length()==1){
			degree=1;						
			outlk.set(outlinks);
			Double d1 = Double.parseDouble(rank)/degree;
			String r=node + "," + d1;
			inrank.set(r);
			context.write(outlk,inrank);
			context.write(nodek, outlk); 
		}
		// for multiple outlinks.
		else{
			
			degree=outlinksm.length;
			for(String st: outlinksm){
				outlk.set(st);
				Double d1 = Double.parseDouble(rank)/degree;
				String r=node + "," + d1;
				inrank.set(r);
				context.write(outlk,inrank);
				context.write(nodek,outlk); 
			}
		}	
		
		
		
	}
	

}

