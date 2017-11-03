package hadoopUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import model.AlgorithmType;
import model.MyItem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import input.CombineDocumentLineInputFormat;

public class MyMapReduceDriver {
	
	public static void deleteOutput(FileSystem hdfs, Path directory) throws FileNotFoundException, IOException {
		FileStatus[] status = hdfs.listStatus(directory);
		for (int i = 0; i < status.length; i++) {
			if (status[i].getPath().getName().startsWith("part-r-")) {
				hdfs.delete(status[i].getPath(), false);
			}
		}
	}

	/**
	 * <h1>���������� �� RTOPk �� ��� hadoop cluster</h1>
	 * 
	 * @param k �� � ��� RTOPk
	 * @param pathS �� path ��� ������� ��� �������� �� ������ S
	 * @param pathW �� path ��� ������� ��� �������� �� ������ W
	 * @param pathOutput �� path ��� �� ����������� �� ����������
	 * @param query �� query
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public void computeRTOPk(int k, Path pathS,
			Path pathW, Path pathOutput, float[] query, AlgorithmType algorithmType, int reducersNo, String jobName,
			int mapMemoryMB, int reduceMemoryMB)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();

		job.setJarByClass(MyMapReduceDriver.class);

		job.setJobName(jobName);
		
		job.getConfiguration().setInt("K", k);
		job.getConfiguration().setInt("reducersNo", reducersNo);
		job.getConfiguration().set("FileName_S", pathS.getName());
		job.getConfiguration().set("FileName_W", pathW.getName());
		job.getConfiguration().setEnum("algorithmType", algorithmType);
		//job.getConfiguration().set("textinputformat.record.delimiter", "\t");
		
		if (mapMemoryMB != -1) {
			int mapperMemory = (int) (((double)mapMemoryMB) * 1.25);
			job.getConfiguration().set("mapreduce.map.memory.mb", mapperMemory + "");
			job.getConfiguration().set("mapreduce.map.java.opts", "-Djava.net.preferIPv4Stack=true -Xmx" + mapMemoryMB + "m");
			//mapreduce.map.java.opts
		}
		
		if (reduceMemoryMB != -1) {
			int reducerMemory = (int) (((double)reduceMemoryMB) * 1.25);
			job.getConfiguration().set("mapreduce.reduce.memory.mb", reducerMemory + "");
			job.getConfiguration().set("mapreduce.reduce.java.opts", "-Djava.net.preferIPv4Stack=true -Xmx" + reduceMemoryMB + "m");
			//mapreduce.reduce.java.opts
		}
		
		long milliSeconds = 1000 * 60 * 60 * 4; //4 hours
		job.getConfiguration().setLong("mapred.task.timeout", milliSeconds);
		
		job.getConfiguration().setInt("queryDimentions", query.length);
		for (int i = 0; i < query.length; i++) {
			job.getConfiguration().setFloat("queryDim" + i, query[i]);
		}

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MyItem.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReducer.class);

		// 64 MB, default block size on hadoop, I did that in order to have
		// locality
		// http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapred/lib/CombineFileInputFormat.html
		CombineDocumentLineInputFormat.setMaxInputSplitSize(job, 134217728);

		job.setInputFormatClass(CombineDocumentLineInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(reducersNo);

		CombineDocumentLineInputFormat.setInputPaths(job, pathS, pathW);

		FileOutputFormat.setOutputPath(job, pathOutput);
		
		long startTime = System.currentTimeMillis();
		@SuppressWarnings("unused")
		boolean success = job.waitForCompletion(true);
		
		long endTime = System.currentTimeMillis();
		
		Path resultsPath = new Path(pathOutput + Path.SEPARATOR + "Results.txt");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		OutputStream out = fs.create(resultsPath).getWrappedStream();
		try {
			out.write(new String("Total time elapsed (ms): " + (endTime - startTime) + "\n").getBytes());
			for (CounterGroup group : job.getCounters()) {
				out.write(new String("* Counter Group" + " \t" + group.getDisplayName() + "\n").getBytes());
				out.write(new String(" number of counters in this group" + " \t" + group.size() + "\n").getBytes());
				for (Counter counter : group) {
					out.write(new String("- "+ counter.getDisplayName() + " \t "+counter.getValue() + "\n").getBytes());
				}
			}
			
			deleteOutput(fs, pathOutput);
			Thread.sleep(5000);
		}
		finally {
			out.close();
			fs.close();
		}
		//System.exit(success ? 0 : 1);
	}

}
