package myPackage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import hadoopUtils.MyMapReduceDriver;

public class DeleteOutputs {

	public static void main(String[] args) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		try {
			FileStatus[] status = fs.listStatus(new Path(args[0]));
			for (int i = 0; i < status.length; i++) {
				if (status[i].isDirectory()) {
					MyMapReduceDriver.deleteOutput(fs, status[i].getPath());
				}
			}
		}
		finally {
			fs.close();
		}
	}

}
