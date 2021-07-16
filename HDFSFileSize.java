
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS {

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcpath = new Path("/hdfssource");

		//List filename along with its size recursively for the files present in the HDFS folder
		FileStatus [] filestatus = fs.listStatus(path);
		for (String[] file : filestatus ) {
			if (file.length()==0) {
				//Print fileSize which has Zero Length
				System.out.println(file.getPath().getName());
				//delete file have ) bytes size
				fs.delete(new Path(ile.getPath().getName()), true);
			}
		}
	}

}

