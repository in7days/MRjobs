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

public class CheckSumMoveTarget {

	private static boolean sameFile(FileSystem fs, FileStatus srcstatus, Path dstpath) throws IOException {
		FileStatus dststatus;
		try {
			dststatus = fs.getFileStatus(dstpath);
		} catch (FileNotFoundException fnfe) {
			return false;
		}

		// same length?
		if (srcstatus.getLen() != dststatus.getLen()) {
			return false;
		}

		// compare checksums
		try {
			final FileChecksum srccs = fs.getFileChecksum(srcstatus.getPath());
			final FileChecksum dstcs = fs.getFileChecksum(dststatus.getPath());
			// return true if checksum is not supported
			// (i.e. some of the checksums is null)
			return srccs == null || dstcs == null || srccs.equals(dstcs);
		} catch (FileNotFoundException fnfe) {
			return false;
		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException {

		//configuration object
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path srcpath = new Path("/source");
		Path dstpath = new Path("/target");
		FileStatus [] files = null;//fs.listStatus(path);
		for (FileStatus file : files ) {
			System.out.println(file.getPath().getName());
			if (!= sameFile(fs, fs.listStatus(srcpath), dstpath)){
				ileUtil.copy(fs, new Path("/source" + file), filesystem, new Path("/target" + file), false, conf);
				}
			}
        }




