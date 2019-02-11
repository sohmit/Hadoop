//Q3

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InMemoryJoin {

	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String,String> map = new HashMap<String,String>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			//String userid = key.toString();
			String[] split_line = line.split("\t");
			if(split_line.length ==2) {
				String userid = split_line[0];
				String[] friends = split_line[1].split(", ");
				//String name = split_line[1];
				List<String> list = new LinkedList<String>();
				for(String f : friends)
				{
					if(map.containsKey(f))
						list.add(map.get(f));
						
				}
				context.write(new Text(userid), new Text(list.toString()));
			
			}}
		
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			
			Configuration conf = context.getConfiguration();
			Path part=new Path(context.getConfiguration().get("ARGUMENT"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
		        	//String[] friends = arr[1].split(", ");
		        	map.put(arr[0], arr[1]+" : "+arr[5]);
		        
		        	line=br.readLine();
		        }
		    }
		}
	}
	
	
	

	public static class Reduce
	extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
			context.write(new Text(key), new Text (values)); 
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("--error message---");
			System.exit(2);
		}
		
		conf.set("ARGUMENT",otherArgs[1]);

		Job job = new Job(conf, "wordid");
		job.setJarByClass(InMemoryJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
