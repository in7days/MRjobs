package com.pavan.mapreduce;

import java.util.HashMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LogPartition extends Partitioner<Text, Text> implements Configurable
{
	  private Configuration configuration;
	  HashMap<String, Integer> months = new HashMap<String, Integer>();
	  
	  public void setConf(Configuration configuration) {
		    this.configuration = configuration;
		    months.put("Jan", 0);
		    months.put("Feb", 1);
		    months.put("Mar", 2);
		    months.put("Apr", 3);
		    months.put("May", 4);
		    months.put("Jun", 5);
		    months.put("Jul", 6);
		    months.put("Aug", 7);
		    months.put("Sep", 8);
		    months.put("Oct", 9);
		    months.put("Nov", 10);
		    months.put("Dec", 11);
		  }
	  
	  public Configuration getConf() {
		    return configuration;
		  }
	  
	  public int getPartition(Text key, Text value, int numReduceTasks) {
		    return (int) (months.get(value.toString()));
		  }
		}







