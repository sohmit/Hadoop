
//Q4

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;  


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin {
	public static class MutualFriendMapper extends
	Mapper<LongWritable, Text, Text, Text> {
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String input = value.toString();
	String[] parts = input.split("\t");
	if(parts.length == 2)
	{
	String[] friends = parts[1].split(",");
	for(int i=0; i<friends.length; i++)
	{
		context.write(new Text(friends[i]) , new Text(parts[0]));
	}
	}
}
}


public static class ReduceJoinReducer extends
	Reducer<Text, Text, Text, Text> {
	
	 static HashMap<String, String> map	;
	
	 @Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		map = new HashMap<String,String>();
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
	        	
	        	if(arr.length == 10) {
	        		String[] date = arr[9].split("/");
	        		String year = date[2];
	        		int age = 2018 - Integer.parseInt(year); 
	        		String str = String.format("%d", age);
	        		String str2 = str+","+arr[3]+","+arr[4]+","+arr[5];
	        	//String[] friends = arr[1].split(", ");
	        	map.put(arr[0].trim(), str2);
	        	}
	        	line=br.readLine();
	        	
	        }
	    }
	}
	
	
public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
	int minAge = 200;
	String str_ = " ";
	String str_2 = " ";
	for(Text t :values) {
		if(map.containsKey(t.toString()))
		{   String val = map.get(t.toString());
			String parts[] = val.split(",");
			if(parts.length > 3)
			{	
				int userAge = Integer.parseInt(parts[0]);	
				//minAge = userAge;
				if(userAge < minAge)
				{
					minAge = userAge;
					str_ = String.format("%d", minAge);
					str_2 = str_+","+parts[1]+","+parts[2]+","+parts[3];
					
				}
				
			}
			
		}
		
	}
	context.write(new Text(key), new Text(str_2));
}

}

public static class FriendMapper2 extends Mapper<Text, Text, LongWritable, Text> {

	  private LongWritable frequency = new LongWritable();

	  public void map(Text key, Text value, Context context)
	    throws IOException, InterruptedException {
		String input = value.toString();
		String parts[] = input.split(",");
		if(parts.length == 4)
		{
			int newVal = Integer.parseInt(parts[0]);
			frequency.set(newVal);
			String str = key.toString() + "\t" + parts[1] + "," + parts[2] + "," + parts[3];
			context.write(frequency, new Text(str));
		}
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






public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


String inputPath = otherArgs[0];
String outputPath = otherArgs[3];
String tempPath = otherArgs[2];

  

{
	conf = new Configuration();
	conf.set("ARGUMENT",otherArgs[1]);
	Job job = new Job(conf, "Reduce-side join");
	job.setJarByClass(ReduceSideJoin.class);
	
	job.setMapperClass(ReduceSideJoin.MutualFriendMapper.class);
	job.setReducerClass(ReduceSideJoin.ReduceJoinReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.setInputPaths(job, new Path(inputPath));
	FileOutputFormat.setOutputPath(job, new Path(tempPath));

	//System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	if(!job.waitForCompletion(true))
        System.exit(1);
}


{
    conf = new Configuration();
    Job job2 = new Job(conf, "Top10");

    job2.setJarByClass(ReduceSideJoin.class);
    job2.setMapperClass(ReduceSideJoin.FriendMapper2.class);
    job2.setReducerClass(ReduceSideJoin.SumReducer2.class);
    
   
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



