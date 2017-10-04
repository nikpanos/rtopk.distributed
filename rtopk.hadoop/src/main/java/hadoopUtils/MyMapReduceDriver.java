package hadoopUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

import grids.gridsS.GridS;
import hadoopUtils.input.CombineDocumentLineInputFormat;
import model.MyItem;
import model.MyKey;

public class MyMapReduceDriver {
	
	private long getCountOfElementsInAntidominanceAreaOfGridS(Configuration conf, String fileName, float[] query, int k, String gridForS) throws IOException {
		Path pt = new Path("hdfs://dnode1:8020/user/pnikitopoulos/" + fileName);
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		GridS gridS = null;
		try {
			gridS = GridS.getGrid(gridForS, br, query, k);
		}
		finally {
			br.close();
		}
		return gridS.getAntidominateAreaCount(query);
	}

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
			int gridWSegmentation, boolean combineFiles, long inputSplitSize, boolean useCombiner,
			int mapMemoryMB, int reduceMemoryMB, String jobName)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();

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
		
		long milliSeconds = 1000 * 60 * 60 * 1; //1 hour
		//job.getConfiguration().setLong("mapreduce.task.timeout", milliSeconds);
		job.getConfiguration().setLong("mapreduce.task.timeout", milliSeconds);
		
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
		
		if (useCombiner) {
			job.setCombinerClass(MyCombiner.class);
			job.setCombinerKeyGroupingComparatorClass(MyCompositeKeyComparator.class);
		}
		
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
			FileInputFormat.setMaxInputSplitSize(job, inputSplitSize);
			
			job.setMapperClass(MyMap.class);
		}
		
		job.setNumReduceTasks(reducersNo);
		//job.setNumReduceTasks(1);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, pathOutput);
		
		//job.getConfiguration().setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
		//job.getConfiguration().setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, org.apache.hadoop.io.compress.SnappyCodec.class, CompressionCodec.class);
		
		long startTime = System.currentTimeMillis();
		
		long countAntidominate = getCountOfElementsInAntidominanceAreaOfGridS(job.getConfiguration(), pathGridS.toString(), query, k, gridForS);
		if (countAntidominate > k) {
			System.out.println("Result set is empty.");
			System.out.println("Antidominate area count: " + countAntidominate);
		}
		else {
			System.out.printf("Items in anti-dominance area of Grid S: %d\n", countAntidominate);
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
			}
			finally {
				out.close();
				fs.close();
			}
		}
	}

}

