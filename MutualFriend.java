


//Q1
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;



public class MutualFriend {
	
	 public static class Map extends MapReduceBase
     implements Mapper<LongWritable, Text, Text, Text>{
     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
             throws IOException{
             
    	 String line = value.toString();
    	 
    	 	//System.out.println("line"  + line);
    		 String[] user = line.split("\t");
    		 
    		 if(user.length == 2)
    		 {
    			 
    			 String[] friends = user[1].split(",");
        		 String[] keys = new String[2];
        		 for(int i=0; i<friends.length; i++)
        		 {
        			 keys[0] = friends[i];	
        			 keys[1] = user[0];
        			 Arrays.sort(keys);
        			 output.collect(new Text(keys[0] + "," + keys[1]), new Text(user[1]));
        			 System.out.println(key.toString()+value.toString());
        			 
        		 }
    			 
    		 }
  
    	    	
     }
}

public static class Reduce extends MapReduceBase
     implements Reducer<Text, Text, Text, Text>{
     public void reduce(Text key, Iterator<Text> values,
     OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
             Text[] commonFriendsList = new Text[2];
             int i = 0;
             while(values.hasNext()){
                     commonFriendsList[i++] = new Text(values.next());
             }
             String[] list1 = commonFriendsList[0].toString().split(",");
             String[] list2 = commonFriendsList[1].toString().split(",");
             List<String> list = new LinkedList<String>();
             for(String f1 : list1){
                     for(String f2 : list2){
                             if(f1.equals(f2)){
                                     list.add(f1);
                             }
                     }
             }
          
             output.collect(key, new Text(list.toString()));
     }
}

public static void main(String[] args) throws Exception{
     JobConf conf = new JobConf(MutualFriend.class);
     conf.setJobName("Friend");

     conf.setMapperClass(Map.class);
     conf.setReducerClass(Reduce.class);

     conf.setMapOutputKeyClass(Text.class);
     conf.setMapOutputValueClass(Text.class);

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(Text.class);

     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

     JobClient.runJob(conf);
}


}


