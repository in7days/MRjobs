package com.pavan.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

public class combiner_partitioner {
    
        public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
                @Override
                public void map(LongWritable key, Text value,
                                Context context) throws IOException, InterruptedException  {
                        String line = value.toString();
                        StringTokenizer tokenizer = new StringTokenizer(line);
                        while (tokenizer.hasMoreTokens()) {
                                value.set(tokenizer.nextToken().toLowerCase());
                                context.write(value, new IntWritable(1));
                        }
                }
        }
        
        public static class MyPartitioner extends Partitioner<Text, IntWritable> {
              
                public int getPartition(Text key, IntWritable value, int numPartitions) {
                        String myKey = key.toString().toLowerCase();
                        if (myKey.equals("hadoop")) {
                                return 0;
                        }
                        if (myKey.equals("data")) {
                                return 1;
                        } else {
                                return 2;
                        }
                }
                
        }
        public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
                @Override
                public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException{
                        int sum = 0;
                        for(IntWritable val: values)

                            {
                                sum += val.get();

                       
                            }
                        
                       context.write(key, new IntWritable(sum));
                }
        }
        
        public static void main(String[] args) throws Exception {
               Configuration conf = new Configuration();
    		   Job job = Job.getInstance(conf, "partitioner");

		job.setJarByClass(combiner_partitioner.class);
job.setNumReduceTasks(3);
                job.setMapperClass(Map.class);
                job.setCombinerClass(Reduce.class);
                job.setReducerClass(Reduce.class);
                job.setPartitionerClass(MyPartitioner.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                
              
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                 
                System.exit(job.waitForCompletion(true)? 0 : 1);
        }
}
