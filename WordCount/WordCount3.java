import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCount3 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {   
	    
            String tok =tokenizer.nextToken();
            if(tok.length()==7){
            word.set(tok);
            context.write(word, one);
	    }
        }
    }
 }
 

 
 public static class Map1 extends Mapper<LongWritable, Text, IntWritable,Text> {
    private Text word = new Text();        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String []tok = line.split("\\s+");
        if(tok.length>=2){
			word.set(tok[0]);
			context.write(new IntWritable(Integer.valueOf(tok[1])), word);
		}
    }
 } 
 public static int c=0;      
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        c=c+1;
        context.write(key, new IntWritable(sum));
    }
 }
 
 public static class Reduce1 extends Reducer< IntWritable,Text, Text,IntWritable> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        for (Text val : values) {
			
		if(c<101){	            
        context.write(val,key);
		}
        c--;
        }
        
    }
 }
   
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "wordcount2");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(WordCount3.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path p1 = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, p1);         
    
    job.waitForCompletion(true);
    Configuration conf1 = new Configuration();
    
    Job job1 = new Job(conf1, "wordcountnew");
    
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(Text.class);
        
    job1.setMapperClass(Map1.class);
    job1.setReducerClass(Reduce1.class);
        
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setNumReduceTasks(1);
    job1.setJarByClass(WordCount3.class);
        
    FileInputFormat.addInputPath(job1, p1);
    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
    job1.waitForCompletion(true);
 }
        
}
