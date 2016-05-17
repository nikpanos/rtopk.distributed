package hadoopUtils;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import hadoopUtils.input.CombineDocumentLineInputFormat;
import model.MyItem;
import model.MyKey;

public class MyMapReduceDriver {

	/**
	 * <h1>Compute the RTOPk in a Hadoop cluster</h1>
	 * 
	 * @param k the � of RTOPk
	 * @param pathS the path of the file which contains the dataset S
	 * @param pathGridS the grid's path of S dataset
	 * @param pathW the path of the file which contains the dataset W
	 * @param pathGridW the grid's path of W dataset
	 * @param pathOutput the path of the results
	 * @param query the query
	 * @param reducersNo the number of the Reducers
	 * @param algorithmForS the algorithm for S that will be used
	 * @param gridForS the grid type for dataset S that will be used
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public void computeRTOPk(int k, Path pathS, Path pathGridS,
			Path pathW, Path pathGridW, Path pathOutput, float[] query, int reducersNo,
			String algorithmForS, String gridForS, String algorithmForRtopk,
			int gridWSegmentation, boolean combineFiles, String jobName)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();

		//job.getConfiguration().set("mapreduce.map.memory.mb", "2560");
		//job.getConfiguration().set("mapreduce.map.java.opts", "-Djava.net.preferIPv4Stack=true -Xmx2147483648");
		//mapreduce.map.java.opts

		//job.getConfiguration().set("mapreduce.reduce.memory.mb", "5632");
		//job.getConfiguration().set("mapreduce.reduce.java.opts", "-Djava.net.preferIPv4Stack=true -Xmx4224m -Xms512m");
		//mapreduce.reduce.java.opts
		
		long milliSeconds = 1000 * 60 * 60 * 3; //3 hours
		//job.getConfiguration().setLong("mapreduce.task.timeout", milliSeconds);
		job.getConfiguration().setLong("mapred.task.timeout", milliSeconds);
		
		job.setJarByClass(MyMapReduceDriver.class);

		job.setJobName(jobName);
		
		job.getConfiguration().setInt("K", k);
		job.getConfiguration().set("FileName_S", pathS.getName());
		job.getConfiguration().set("FileName_W", pathW.getName());
		job.getConfiguration().set("AlgorithmForS", algorithmForS);
		job.getConfiguration().set("AlgorithmForRtopk", algorithmForRtopk);
		job.getConfiguration().set("GridForS", gridForS);
		job.getConfiguration().setInt("gridWSegmentation", gridWSegmentation);
		job.addCacheFile( pathGridS.toUri() );
		job.addCacheFile( pathGridW.toUri() );
		
		job.getConfiguration().setInt("queryDimentions", query.length);
		for (int i = 0; i < query.length; i++) {
			job.getConfiguration().setFloat("queryDim" + i, query[i]);
		}

		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(MyItem.class);

		job.setPartitionerClass(MyPartitioner.class);
		job.setGroupingComparatorClass(MyCompositeKeyComparator.class);
		//job.setSortComparatorClass(MyCompositeKeyComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		job.setReducerClass(MyReducer.class);
		
		if (combineFiles) {
			// 64 MB, default block size on hadoop, I did that in order to have
			// locality
			// http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapred/lib/CombineFileInputFormat.html
			CombineDocumentLineInputFormat.setMaxInputSplitSize(job, 67108864);

			job.setInputFormatClass(CombineDocumentLineInputFormat.class);
			CombineDocumentLineInputFormat.setInputPaths(job, pathS, pathW);
			
			job.setMapperClass(MyCombineMapper.class);
		}
		else {
			FileInputFormat.addInputPath(job, pathS);
			FileInputFormat.addInputPath(job, pathW);
			FileInputFormat.setMaxInputSplitSize(job, 33554432);
			
			job.setMapperClass(MyMap.class);
		}
		
		job.setNumReduceTasks(reducersNo);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, pathOutput);

		@SuppressWarnings("unused")
		boolean success = job.waitForCompletion(true);
		
		Path resultsPath = new Path(pathOutput + Path.SEPARATOR + "Results.txt");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		OutputStream out = fs.create(resultsPath).getWrappedStream();
		
		for (CounterGroup group : job.getCounters()) {
			out.write(new String("* Counter Group" + " \t" + group.getDisplayName() + "\n").getBytes());
			out.write(new String(" number of counters in this group" + " \t" + group.size() + "\n").getBytes());
			for (Counter counter : group) {
				out.write(new String("- "+ counter.getDisplayName() + " \t "+counter.getValue() + "\n").getBytes());
			}
		}
		out.close();
	}

}

