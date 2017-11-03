package myPackage;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import model.AlgorithmType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import hadoopUtils.MyMapReduceDriver;

public class Main {
	
	/*
	 * args[0] = k
	 * 
	 * args[1] = query point (comma separated)
	 * 
	 * args[2] = path dataset S
	 * 
	 * args[3] = path dataset W
	 * 
	 * args[4] = path output
	 * 
	 * args[5] = num reducers
	 * 
	 * args[6] = algorithm type (0 for RTA, 1 for BRS)
	 * 
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		/*int k = 10;
		float[] query = new float[] { 0.03f, 0.2f };
		Path pathS = new Path("hdfs://localhost:19000/dataFinal/S");
		Path pathW = new Path("hdfs://localhost:19000/dataFinal/W");
		Path pathOutput = new Path("hdfs://localhost:19000/DataFinalNew/Results_FULLD");
		int reducersNo = 1;
		AlgorithmType type = AlgorithmType.NoSortingW;
		
		//if (args.length == 6) {
			k = Integer.parseInt(args[0]);
			String[] splitQuery = args[1].split(",");
			query = new float[splitQuery.length];
			for (int i = 0; i < splitQuery.length; i++) {
				query[i] = Float.parseFloat(splitQuery[i]);
			}
			pathS = new Path(args[2]);
			pathW = new Path(args[3]);
			pathOutput = new Path(args[4]);
			reducersNo = Integer.parseInt(args[5]);
			int tmp = Integer.parseInt(args[6]);
			switch (tmp) {
			case 0:
				type = AlgorithmType.NoSortingW;
				break;
			case 1:
				type = AlgorithmType.BRS;
				break;
			}
		//}
		
		MyMapReduceDriver driver = new MyMapReduceDriver();
		driver.computeRTOPk(k, pathS, pathW, pathOutput, query, type, reducersNo);*/
		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] status = fs.listStatus(new Path(args[0]));
		for (int i = 0; i < status.length; i++) {
			try {
				executeExpirement(status[i].getPath());
			} catch (Exception e){
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	private static void executeExpirement(Path path) throws IOException, ClassNotFoundException, InterruptedException {
		FileSystem fs = FileSystem.get(new Configuration());

		InputStreamReader fileInput = new InputStreamReader(fs.open(path));
		Properties properties = new Properties();
		properties.load(fileInput);
		fileInput.close();

		int k = Integer.parseInt(properties.getProperty("K"));
		String[] arrayString = properties.getProperty("Query").replace("[", "").replace("]", "").trim().split(",");
		float[] query = new float[arrayString.length];
		for (int i = 0; i < arrayString.length; i++) {
			query[i] = Float.parseFloat(arrayString[i]);
		}

		Path pathS = new Path(properties.getProperty("PathS"));
		//Path pathGridS = new Path(properties.getProperty("PathGridS"));
		Path pathW = new Path(properties.getProperty("PathW"));
		//Path pathGridW = new Path(properties.getProperty("PathGridW"));
		Path pathOutput = new Path(properties.getProperty("PathOutput"));

		int reducersNo = Integer.parseInt(properties.getProperty("ReducersNumber"));
		//String algorithmForS = properties.getProperty("AlgorithmForS");
		//String algorithmForRtopk = properties.getProperty("AlgorithmForRtopk");
		//String gridForS = properties.getProperty("GridForS");
		String jobName = properties.getProperty("JobName");
		//int gridWSegmentation = Integer.parseInt(properties.getProperty("gridWSegmentation"));
		//boolean combineFiles = Boolean.parseBoolean(properties.getProperty("combineFiles"));
		//long inputSplitSize = Long.parseLong(properties.getProperty("inputSplitSize"));
		//boolean useCombiner = Boolean.parseBoolean(properties.getProperty("useCombiner"));
		int mapMemoryMB = -1;
		int reduceMemoryMB = -1;
		if (properties.getProperty("mapMemoryMB") != null) {
			mapMemoryMB = Integer.parseInt(properties.getProperty("mapMemoryMB"));
		}
		if (properties.getProperty("reduceMemoryMB") != null) {
			reduceMemoryMB = Integer.parseInt(properties.getProperty("reduceMemoryMB"));
		}

		MyMapReduceDriver driver = new MyMapReduceDriver();
		driver.computeRTOPk(k, pathS, pathW, pathOutput, query, AlgorithmType.BRS, reducersNo, jobName,
				mapMemoryMB, reduceMemoryMB);

	}

}
