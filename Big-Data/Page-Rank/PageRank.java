/**
 * Driver class for Mapreduce - for calculating the page rank through 10 iterationa
 */




import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PageRank {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		for(int i=0;i<10;i++)
		{
			
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1, "PageRank");
		job1.setJarByClass(PageRank.class);
        job1.setMapperClass(PageRankMapper.class);
        job1.setReducerClass(PageRankReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[i]));
        FileOutputFormat.setOutputPath(job1, new Path(args[i+1]));
        job1.waitForCompletion(true);
        
        
		}
		
	}

}
