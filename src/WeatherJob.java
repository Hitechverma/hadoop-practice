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
        
public class WeatherJob {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable temperature = new IntWritable(1);
    private Text weatherKey = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");

        weatherKey.set(tokens[0]);
        temperature.set(Integer.parseInt(tokens[3]));
        context.write(weatherKey,temperature);

        weatherKey.set(tokens[0]+tokens[1]);
        temperature.set(Integer.parseInt(tokens[3]));
        context.write(weatherKey,temperature);
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {

        long sum = 0;
        long count = 0;
        long min = 0;
        long max = 0;

        for (IntWritable val : values) {
            int temperature = val.get();

            sum += temperature;
            count++;

            if (min > temperature)
            {
               min = temperature;
            }

            if (max < temperature)
            {
               max = temperature;
            }
        }

        
        context.write(new Text(key.toString() + " - Avg"), new IntWritable((int)(sum/count)));
        context.write(new Text(key.toString() + " - Min"), new IntWritable((int)min));
        context.write(new Text(key.toString() + " - Max"), new IntWritable((int)max));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "WeatherJob");
    
    job.setJarByClass(WeatherJob.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}

