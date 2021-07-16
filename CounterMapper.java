package com.pavan.mapreduce;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class CounterMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {
	enum InfoCounter{EMAIL,MOBILE};

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
    String[] fields = value.toString().split(",");
    if (fields.length > 1) {
      int email = Integer.parseInt(fields[10]);
      int mobile = Integer.parseInt(fields[11]);
      if (email==1) {
          context.getCounter(InfoCounter.EMAIL).increment(1);
       }
      if (mobile==1) {
    	  context.getCounter(InfoCounter.MOBILE).increment(1);
      }
    }
  }
}
