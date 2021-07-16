package com.pavan.mapreduce;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CounterDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new Configuration(), new CounterDriver(), args);
	    System.exit(exitCode);
	  }

 
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: InfoCounter <input dir> <output dir>\n");
      return -1;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(CounterDriver.class);
    job.setJobName("Info Counter");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // This is a map-only job, so we do not call setReducerClass.
    job.setMapperClass(CounterMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    /*
     * Set the number of reduce tasks to 0. 
     */
    job.setNumReduceTasks(0);

    boolean success = job.waitForCompletion(true);
    if (success) {
      /*
       * Print out the counters that the mappers have been incrementing.
       */
      long email = job.getCounters().findCounter(CounterMapper.InfoCounter.EMAIL).getValue();
      long mobile = job.getCounters().findCounter(CounterMapper.InfoCounter.MOBILE).getValue();
 
      System.out.println("Email   = " + email);
      System.out.println("Mobile   = " + mobile);
      return 0;
    } else
      return 1;
  }

  
}


