package myPackage;

import hadoopUtils.MyMapReduceDriver;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

	/**
	 * 
	 * args[0] - file with job Variables
	 * 
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException {

		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] status = fs.listStatus(new Path(args[0]));
		for (int i = 0; i < status.length; i++) {
			try {
				executeExpirement(status[i].getPath());
			} catch (Exception e){
				continue;
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
		Path pathGridS = new Path(properties.getProperty("PathGridS"));
		Path pathW = new Path(properties.getProperty("PathW"));
		Path pathGridW = new Path(properties.getProperty("PathGridW"));
		Path pathOutput = new Path(properties.getProperty("PathOutput"));

		int reducersNo = Integer.parseInt(properties.getProperty("ReducersNumber"));
		String algorithmForS = properties.getProperty("AlgorithmForS");
		String algorithmForRtopk = properties.getProperty("AlgorithmForRtopk");
		String gridForS = properties.getProperty("GridForS");
		String jobName = properties.getProperty("JobName");
		int gridWSegmentation = Integer.parseInt(properties.getProperty("gridWSegmentation"));
		boolean combineFiles = Boolean.parseBoolean(properties.getProperty("combineFiles"));

		MyMapReduceDriver driver = new MyMapReduceDriver();
		driver.computeRTOPk(k, pathS, pathGridS, pathW, pathGridW, pathOutput, query, reducersNo, algorithmForS,
				gridForS, algorithmForRtopk, gridWSegmentation, combineFiles, jobName);

	}

}
