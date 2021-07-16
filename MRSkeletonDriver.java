package com.pavan.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MRSkeletonDriver {

	
	public class MyMapper extends Mapper <LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context )
		{
			//MSBL Dp Logic
			// mynewk, mynewv
			// context.write(mynewk, mynewv)
		}
	}
	
	public class MyReducer extends Reducer <Text, Text, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<Text> values, Context context)
		{
			// ABL
			// mynewk, mynewv
			// write it into context
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MRSkeleton for batch233");
		
		//Mapper reducer and driver
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(MRSkeletonDriver.class);
		
		
		
		// Mok, Mov		Rok		Rov  
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// input format and outptu format
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}

}
