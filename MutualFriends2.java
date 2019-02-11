//Q2

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriends2 {
	
	
	public static class FriendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		  IntWritable one = new IntWritable(1);
		  Text text = new Text();

		  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		   String line = value.toString();
		   String split_line[] = line.split("\t");
		   if(split_line.length == 2)
		   {
			   String keys = split_line[0];
			   String[] friends = split_line[1].split(",");
			   for(int i =0; i<friends.length ; i++)
			   {
				   context.write(new Text(keys), one);
			   }
		   
			  
		   }
		} 

	}
	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable count = new IntWritable();

	
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		    int friendCount = 0;
		    for (IntWritable value : values) {
		        friendCount += value.get();
		    }
		    count.set(friendCount);
		    context.write(key, count);
		  }
		}
	
	public static class FriendMapper2 extends Mapper<Text, Text, LongWritable, Text> {

		  private LongWritable frequency = new LongWritable();

		  public void map(Text key, Text value, Context context)
		    throws IOException, InterruptedException {

		    int newVal = Integer.parseInt(value.toString());
		    frequency.set(newVal);
		    context.write(frequency, key);
		  }
		}
	
	
	public static class SumReducer2 extends Reducer<LongWritable, Text, Text, LongWritable> {
		private int idx = 0;


		 
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
			
			for (Text value : values) {
		    	if (idx < 10) {
		    		idx++;
		    		context.write(value, key);
		    	}
		    }
		  }
		}
	
	
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];
        String tempPath = otherArgs[2];
       
        {	
            conf = new Configuration();
            Job job = new Job(conf, "CountFriends");

            job.setJarByClass(MutualFriends2.class);
            job.setMapperClass(MutualFriends2.FriendMapper.class);
            job.setReducerClass(MutualFriends2.SumReducer.class);
            
            
            job.setMapOutputKeyClass(Text.class);
           
            job.setMapOutputValueClass(IntWritable.class);
            
            
            job.setOutputKeyClass(Text.class);
            
            job.setOutputValueClass(IntWritable.class);
            
            FileInputFormat.addInputPath(job, new Path(inputPath));
           
            FileOutputFormat.setOutputPath(job, new Path(tempPath));

            if(!job.waitForCompletion(true))
                System.exit(1);
        }
        
        {
            conf = new Configuration();
            Job job2 = new Job(conf, "Top10Friends");

            job2.setJarByClass(MutualFriends2.class);
            job2.setMapperClass(MutualFriends2.FriendMapper2.class);
            job2.setReducerClass(MutualFriends2.SumReducer2.class);
            
           
            job2.setMapOutputKeyClass(LongWritable.class);
            
            job2.setMapOutputValueClass(Text.class);
            
           
            job2.setOutputKeyClass(Text.class);
            
            job2.setOutputValueClass(LongWritable.class);

            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            
           
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job2.setNumReduceTasks(1);
         
            FileInputFormat.addInputPath(job2, new Path(tempPath));
         
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
