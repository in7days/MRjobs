package com.pavan.mapreduce;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class WriteHDFS {

	public static void main(String[] args) throws IOException, URISyntaxException {	
		
		//configuration object
		Configuration conf = new Configuration();
		
		//to read options from commandline
		String otherArgs[] = new GenericOptionsParser(args).getRemainingArgs();
		String localfile = otherArgs[0]; //home/training/stocks
		String hdfsfile = otherArgs[1]; //hdfs/mystocks
		
		//create a object of Local file system
		FileSystem lfs = FileSystem.getLocal(conf);
		
		//create a object of hdfs filesystem object
		FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:8020"), conf);
		
		// this will read file from local file syste
		FSDataInputStream in = lfs.open(new Path(localfile));
		
		//I need to write it into HDFS
		FSDataOutputStream out = hdfs.create(new Path(hdfsfile));
		
		byte[] buffer = new byte[256];
		while(in.read(buffer) > 0)
		{
			out.write(buffer, 0, 256);
		}
		out.close();
		in.close();
	}
	
}

