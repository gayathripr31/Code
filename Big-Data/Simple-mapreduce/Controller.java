import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class FBBrandTime {
	

	public static void main(String[] args) throws Exception , ArrayIndexOutOfBoundsException{
		// Configuration hold information about jobtracker, input, output etc
		Configuration con1 = new Configuration();
		// created job1 for first job
        Job job1 = new Job(con1, "FBBrandTime");
        //The jar file is specified
        job1.setJarByClass(FBBrandTime.class);
        
        
        //Type of Key and Value
        //Here it is Text
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
 
        //The Mapperclass for first job
        job1.setMapperClass(Mapper1.class);
        //Partitioner class for first job
        //job1.setPartitionerClass(UserPartition.class);
        //Number of reducers for job1
        job1.setNumReduceTasks(3);
        //Reducer class specified
        job1.setReducerClass(Reducer1.class);
        //Input and Output format for the job
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        //Add path for input path for map reduce job
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        //Add path for output path for map reduce job
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
 
        //Submit the job to the cluster and wait for it to finish
        job1.waitForCompletion(true);
        // Configuration hold information about jobtracker, input, output etc
        Configuration con2 = new Configuration();
        // created job2 for first job
        Job job2 = new Job(con2, "FBBrandPair");
        //The jar file is specified
        job2.setJarByClass(FBBrandTime.class);
        //Type of Key and Value
        //Here it is Text
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        //The Mapperclass for first job
        job2.setMapperClass(Mapper2.class);
        //Reducer class specified
        job2.setReducerClass(Reducer2.class);
        //Input and Output format for the job
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        //Add the output path of first reducer as the input path of first mapper
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        //Add path for output path for map reduce job
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        //Submit the job to the cluster and wait for it to finish
        job2.waitForCompletion(true);
		
	}

}
