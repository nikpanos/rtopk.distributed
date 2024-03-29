package hadoopUtils.input;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * A base class for file-based {@link InputFormat}s.
 * 
 * <p>
 * <code>FileInputFormat</code> is the base class for all file-based
 * <code>InputFormat</code>s. This provides a generic implementation of
 * {@link #getSplits(JobContext)}. Subclasses of <code>FileInputFormat</code>
 * can also override the {@link #isSplitable(JobContext, Path)} method to ensure
 * input-files are not split-up and are processed as a whole by {@link Mapper}s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileInputFormatBalancedFileSplitSize<K, V> extends InputFormat<K, V> {
	public static final String INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";
	public static final String SPLIT_MAXSIZE = "mapreduce.input.fileinputformat.split.maxsize";
	public static final String SPLIT_MINSIZE = "mapreduce.input.fileinputformat.split.minsize";
	public static final String PATHFILTER_CLASS = "mapreduce.input.pathFilter.class";
	public static final String NUM_INPUT_FILES = "mapreduce.input.fileinputformat.numinputfiles";
	public static final String INPUT_DIR_RECURSIVE = "mapreduce.input.fileinputformat.input.dir.recursive";
	public static final String LIST_STATUS_NUM_THREADS = "mapreduce.input.fileinputformat.list-status.num-threads";
	public static final int DEFAULT_LIST_STATUS_NUM_THREADS = 1;

	private static final Log LOG = LogFactory.getLog(FileInputFormatBalancedFileSplitSize.class);

	//private static final double SPLIT_SLOP = 1.1; // 10% slop

	@Deprecated
	public static enum Counter {
		BYTES_READ
	}

	private static final PathFilter hiddenFileFilter = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};

	/**
	 * Proxy PathFilter that accepts a path only if all filters given in the
	 * constructor do. Used by the listPaths() to apply the built-in
	 * hiddenFileFilter together with a user provided one (if any).
	 */
	private static class MultiPathFilter implements PathFilter {
		private List<PathFilter> filters;

		public MultiPathFilter(List<PathFilter> filters) {
			this.filters = filters;
		}

		public boolean accept(Path path) {
			for (PathFilter filter : filters) {
				if (!filter.accept(path)) {
					return false;
				}
			}
			return true;
		}
	}

	/**
	 * @param job
	 *            the job to modify
	 * @param inputDirRecursive
	 */
	public static void setInputDirRecursive(Job job, boolean inputDirRecursive) {
		job.getConfiguration().setBoolean(INPUT_DIR_RECURSIVE,
				inputDirRecursive);
	}

	/**
	 * @param job
	 *            the job to look at.
	 * @return should the files to be read recursively?
	 */
	public static boolean getInputDirRecursive(JobContext job) {
		return job.getConfiguration().getBoolean(INPUT_DIR_RECURSIVE, false);
	}

	/**
	 * Get the lower bound on split size imposed by the format.
	 * 
	 * @return the number of bytes of the minimal split for this format
	 */
	protected long getFormatMinSplitSize() {
		return 1;
	}

	/**
	 * Is the given filename splitable? Usually, true, but if the file is stream
	 * compressed, it will not be.
	 * 
	 * <code>FileInputFormat</code> implementations can override this and return
	 * <code>false</code> to ensure that individual input files are never
	 * split-up so that {@link Mapper}s process entire files.
	 * 
	 * @param context
	 *            the job context
	 * @param filename
	 *            the file name to check
	 * @return is this file splitable?
	 */
	protected boolean isSplitable(JobContext context, Path filename) {
		return true;
	}

	/**
	 * Set a PathFilter to be applied to the input paths for the map-reduce job.
	 * 
	 * @param job
	 *            the job to modify
	 * @param filter
	 *            the PathFilter class use for filtering the input paths.
	 */
	public static void setInputPathFilter(Job job,
			Class<? extends PathFilter> filter) {
		job.getConfiguration().setClass(PATHFILTER_CLASS, filter,
				PathFilter.class);
	}

	/**
	 * Set the minimum input split size
	 * 
	 * @param job
	 *            the job to modify
	 * @param size
	 *            the minimum size
	 */
	public static void setMinInputSplitSize(Job job, long size) {
		job.getConfiguration().setLong(SPLIT_MINSIZE, size);
	}

	/**
	 * Get the minimum split size
	 * 
	 * @param job
	 *            the job
	 * @return the minimum number of bytes that can be in a split
	 */
	public static long getMinSplitSize(JobContext job) {
		return job.getConfiguration().getLong(SPLIT_MINSIZE, 1L);
	}

	/**
	 * Set the maximum split size
	 * 
	 * @param job
	 *            the job to modify
	 * @param size
	 *            the maximum split size
	 */
	public static void setMaxInputSplitSize(Job job, long size) {
		job.getConfiguration().setLong(SPLIT_MAXSIZE, size);
	}

	/**
	 * Get the maximum split size.
	 * 
	 * @param context
	 *            the job to look at.
	 * @return the maximum number of bytes a split can include
	 */
	public static long getMaxSplitSize(JobContext context) {
		return context.getConfiguration()
				.getLong(SPLIT_MAXSIZE, Long.MAX_VALUE);
	}

	/**
	 * Get a PathFilter instance of the filter set for the input paths.
	 *
	 * @return the PathFilter instance set for the job, NULL if none has been
	 *         set.
	 */
	public static PathFilter getInputPathFilter(JobContext context) {
		Configuration conf = context.getConfiguration();
		Class<?> filterClass = conf.getClass(PATHFILTER_CLASS, null,
				PathFilter.class);
		return (filterClass != null) ? (PathFilter) ReflectionUtils
				.newInstance(filterClass, conf) : null;
	}

	/**
	 * List input directories. Subclasses may override to, e.g., select only
	 * files matching a regular expression.
	 * 
	 * @param job
	 *            the job to list input paths for
	 * @return array of FileStatus objects
	 * @throws IOException
	 *             if zero items.
	 */
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		Path[] dirs = getInputPaths(job);
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}

		// get tokens for all the required FileSystems..
		TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs,
				job.getConfiguration());

		// Whether we need to recursive look into the directory structure
		boolean recursive = getInputDirRecursive(job);

		// creates a MultiPathFilter with the hiddenFileFilter and the
		// user provided one (if any).
		List<PathFilter> filters = new ArrayList<PathFilter>();
		filters.add(hiddenFileFilter);
		PathFilter jobFilter = getInputPathFilter(job);
		if (jobFilter != null) {
			filters.add(jobFilter);
		}
		PathFilter inputFilter = new MultiPathFilter(filters);

		List<FileStatus> result = null;

		int numThreads = job.getConfiguration().getInt(LIST_STATUS_NUM_THREADS,
				DEFAULT_LIST_STATUS_NUM_THREADS);
		Stopwatch sw = new Stopwatch().start();
		if (numThreads == 1) {
			result = singleThreadedListStatus(job, dirs, inputFilter, recursive);
		} else {
			Iterable<FileStatus> locatedFiles = null;
			try {
				LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
						job.getConfiguration(), dirs, recursive, inputFilter,
						true);
				locatedFiles = locatedFileStatusFetcher.getFileStatuses();
			} catch (InterruptedException e) {
				throw new IOException("Interrupted while getting file statuses");
			}
			result = Lists.newArrayList(locatedFiles);
		}

		sw.stop();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Time taken to get FileStatuses: " + sw.elapsedMillis());
		}
		LOG.info("Total input paths to process : " + result.size());
		return result;
	}

	private List<FileStatus> singleThreadedListStatus(JobContext job,
			Path[] dirs, PathFilter inputFilter, boolean recursive)
			throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();
		List<IOException> errors = new ArrayList<IOException>();
		for (int i = 0; i < dirs.length; ++i) {
			Path p = dirs[i];
			FileSystem fs = p.getFileSystem(job.getConfiguration());
			FileStatus[] matches = fs.globStatus(p, inputFilter);
			if (matches == null) {
				errors.add(new IOException("Input path does not exist: " + p));
			} else if (matches.length == 0) {
				errors.add(new IOException("Input Pattern " + p
						+ " matches 0 files"));
			} else {
				for (FileStatus globStat : matches) {
					if (globStat.isDirectory()) {
						RemoteIterator<LocatedFileStatus> iter = fs
								.listLocatedStatus(globStat.getPath());
						while (iter.hasNext()) {
							LocatedFileStatus stat = iter.next();
							if (inputFilter.accept(stat.getPath())) {
								if (recursive && stat.isDirectory()) {
									addInputPathRecursively(result, fs,
											stat.getPath(), inputFilter);
								} else {
									result.add(stat);
								}
							}
						}
					} else {
						result.add(globStat);
					}
				}
			}
		}

		if (!errors.isEmpty()) {
			throw new InvalidInputException(errors);
		}
		return result;
	}

	/**
	 * Add files in the input path recursively into the results.
	 * 
	 * @param result
	 *            The List to store all files.
	 * @param fs
	 *            The FileSystem.
	 * @param path
	 *            The input path.
	 * @param inputFilter
	 *            The input filter that can be used to filter files/dirs.
	 * @throws IOException
	 */
	protected void addInputPathRecursively(List<FileStatus> result,
			FileSystem fs, Path path, PathFilter inputFilter)
			throws IOException {
		RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
		while (iter.hasNext()) {
			LocatedFileStatus stat = iter.next();
			if (inputFilter.accept(stat.getPath())) {
				if (stat.isDirectory()) {
					addInputPathRecursively(result, fs, stat.getPath(),
							inputFilter);
				} else {
					result.add(stat);
				}
			}
		}
	}

	/**
	 * Generate the list of files and make them into FileSplits.
	 * 
	 * @param job
	 *            the job context
	 * @throws IOException
	 */
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		// �������� � ������������ ��� �����������
		Stopwatch sw = new Stopwatch().start();

		// �������� �� �������� ������� ��� split
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		// �������� �� ������� ������� ��� split
		long maxSize = getMaxSplitSize(job);

		// �������������� ��� ����� �� �� splits
		List<CombineFileSplit> splits = new ArrayList<CombineFileSplit>();

		// �������� � ����� �� ��� �� ������
		List<FileStatus> files = listStatus(job);

		// �������� �� path ���
		Path path[] = new Path[files.size()];
		// �������� �� ����� ��� �� bytes
		long[] length = new long[files.size()];
		// �� ������� ��� ����������� ��� ������� ��� split ��� ���� ������
		// ��. �� �� ������ x1 = 150 MB ��� �� x2 = 50 MB ���� �� ������� ��� ������� ��� split
		// ��� �� ������ x1 = 2/3 ��� ��� �� x2 = 1/3. ��� �� �� split size ����� 30 MB ���� ���� split
		// �� �������� 20 MB ��� �� x1 ������ ��� 10 MB ��� �� x2 ������.
		double[] weightSplitSize = new double[files.size()];
		// �� �������� ��� ������� ��� ��� ������� ����������� �� ����������� 
		// �� ������� ��� ����������� ��� ������� ��� split ��� ���� ������
		long totalbytes = 0;

		List<BlockLocation[]> filesBlockLocations = new ArrayList<BlockLocation[]>();

		for (int i = 0; i < files.size(); i++) {
			path[i] = files.get(i).getPath();
			length[i] = files.get(i).getLen();
			// ������������ �� �������� ��� ������� ��� ��� �������
			totalbytes += length[i];
			// ������������� ���� ������ �� �� BlockLocation ���
			// ������������ ��� �������� ��������� ��� block, �����������
			// ������� �� ����
			// hosts ��� ��������� �� block replicas, ��� ���� block
			// metadata
			// (E.g. the file offset associated with the block, length,
			// whether it
			// is corrupt, etc).
			BlockLocation[] blkLocations;

			// �� �� ������ ����� ��� instance ��� ������ LocatedFileStatus
			if (files.get(i) instanceof LocatedFileStatus) {
				// ���� ��� ���������� ��� block ��� �������
				blkLocations = ((LocatedFileStatus) files.get(i))
						.getBlockLocations();

			}
			// �� �� ������ ��� ����� ��� instance ��� ������
			// LocatedFileStatus
			else {
				// ������� �� FileSystem ��� ����� ������ �� Path (���. ��
				// ������)
				FileSystem fs = path[i].getFileSystem(job.getConfiguration());

				// Return an array containing hostnames, offset and size of
				// portions of the given file. For a nonexistent file or
				// regions, null will be returned. This call is most helpful
				// with DFS, where it returns hostnames of machines that
				// contain the given file. The FileSystem will simply return
				// an elt containing 'localhost'.
				blkLocations = fs.getFileBlockLocations(files.get(i), 0,
						length[i]);
			}

			filesBlockLocations.add(i,Arrays.copyOf(blkLocations,blkLocations.length));

		}

		// �� �� ������� ��� ������� ��� ����� 0 ����...
		if (length[0] != 0 && length[1] != 0) {

			// �� ����������� � �������� ��� �������, ����...
			if (isSplitable(job, path[0]) && isSplitable(job, path[1])) {

				// ������� �� ������� ��� Block ��� �������
				long[] blockSize = new long[files.size()];

				// ������� �� ������� ��� split ��� �������
				long[] splitSize = new long[files.size()];

				// ����� �� bytes ��� ��������� (���. ��� ����� ���� ��
				// ������ split �����)
				long[] bytesRemaining = new long[files.size()];

				long[] startFrom = new long[files.size()];

				for (int i = 0; i < files.size(); i++) {
					blockSize[i] = files.get(i).getBlockSize();
					// ���������� �� ������� ��� ��� ����� ��� ����������� ��� �� i ������ ��� splitSize 
					weightSplitSize[i] = (double) length[i]/totalbytes;
					//System.out.println("                        ------ split size percentage: " + weightSplitSize[i]);
					splitSize[i] = computeSplitSize(blockSize[i], minSize,
							maxSize,weightSplitSize[i]);

					// ���� ���� ����� ��� �� �� ������� ��� ������� ����� ���
					// ���� "split�������" �����
					bytesRemaining[i] = length[i];

					startFrom[i] = length[i] - bytesRemaining[i];
				}

				// ������ �� �� ������ ���� ���� �� ������� ��� ���� ����� ���
				// ������� ��� ��������� ��� ��� �� �� ���� ���� ��������
				// ���������.
				int[] blkIndex = new int[files.size()];
				blkIndex[0] = getBlockIndex(filesBlockLocations.get(0),
						startFrom[0]);
				blkIndex[1] = getBlockIndex(filesBlockLocations.get(1),
						startFrom[1]);

				for (String a : filesBlockLocations.get(0)[blkIndex[0]]
						.getHosts())
					System.out.println("   + " + a);

				while (bytesRemaining[0] > 0 || bytesRemaining[1] > 0) {

					splits.add(new CombineFileSplit(Arrays.copyOf(path,
							path.length), Arrays.copyOf(startFrom,
							startFrom.length), Arrays.copyOf(splitSize,
							splitSize.length),
							filesBlockLocations.get(0)[blkIndex[0]].getHosts()));

					// ������ �� ������������� bytes ���� splitSize,
					// ����� ���� bytes ������ ��� split
					for (int i = 0; i < files.size(); i++) {
						bytesRemaining[i] -= splitSize[i];
						startFrom[i] = length[i] - bytesRemaining[i];
						if (bytesRemaining[i] < splitSize[i])
							splitSize[i] = bytesRemaining[i];
						if (bytesRemaining[i] <= 0) {
							splitSize[i] = 0;
							bytesRemaining[i] = 0;
						}
					}
				}

			}
			// �� ��� ����������� � �������� ��� �������, ����...
			else {
				long[] zeros = new long[1];
				zeros[0] = 0;
				// ���� �� ��� split ��� �� ������
				splits.add(new CombineFileSplit(path, zeros, length,
						filesBlockLocations.get(0)[0].getHosts()));
			}
		} else {
			// Create empty hosts array for zero length files
			long[] zeros = new long[1];
			zeros[0] = 0;
			splits.add(new CombineFileSplit(path, zeros, length, new String[0]));
		}

		// Save the number of input files for metrics/loadgen
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		sw.stop();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Total # of splits generated by getSplits: "
					+ splits.size() + ", TimeTaken: " + sw.elapsedMillis());
		}

		List<InputSplit> results = new ArrayList<InputSplit>();

		System.out
				.println("  ______________________________________________________\n");

		for (CombineFileSplit a : splits) {
			System.out.println("   - CombineFileSplit: " + a);
			results.add(a);
		}

		for (InputSplit a : results) {
			System.out.println("   - InputSplit: " + a.toString());
		}

		return results;
	}

	/**
	 * 
	 * @param blockSize
	 * @param minSize
	 * @param maxSize
	 * @param weightSplitSize �� ������� ��� ����������� ��� ������ ��� split
	 * @return
	 */
	protected long computeSplitSize(long blockSize, long minSize, long maxSize, double weightSplitSize) {
		// ���������� �� SplitSize ��� ����������� ��� ������ ���� �� weightSplitSize
		return Math.round(Math.max(minSize, Math.min(maxSize, blockSize))*weightSplitSize);
	}

	protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
		for (int i = 0; i < blkLocations.length; i++) {
			// is the offset inside this block?
			if ((blkLocations[i].getOffset() <= offset)
					&& (offset < blkLocations[i].getOffset()
							+ blkLocations[i].getLength())) {
				return i;
			}
		}
		BlockLocation last = blkLocations[blkLocations.length - 1];
		long fileLength = last.getOffset() + last.getLength() - 1;
		throw new IllegalArgumentException("Offset " + offset
				+ " is outside of file (0.." + fileLength + ")");
	}

	/**
	 * Sets the given comma separated paths as the list of inputs for the
	 * map-reduce job.
	 * 
	 * @param job
	 *            the job
	 * @param commaSeparatedPaths
	 *            Comma separated paths to be set as the list of inputs for the
	 *            map-reduce job.
	 */
	public static void setInputPaths(Job job, String commaSeparatedPaths)
			throws IOException {
		setInputPaths(job,
				StringUtils.stringToPath(getPathStrings(commaSeparatedPaths)));
	}

	/**
	 * Add the given comma separated paths to the list of inputs for the
	 * map-reduce job.
	 * 
	 * @param job
	 *            The job to modify
	 * @param commaSeparatedPaths
	 *            Comma separated paths to be added to the list of inputs for
	 *            the map-reduce job.
	 */
	public static void addInputPaths(Job job, String commaSeparatedPaths)
			throws IOException {
		for (String str : getPathStrings(commaSeparatedPaths)) {
			addInputPath(job, new Path(str));
		}
	}

	/**
	 * Set the array of {@link Path}s as the list of inputs for the map-reduce
	 * job.
	 * 
	 * @param job
	 *            The job to modify
	 * @param inputPaths
	 *            the {@link Path}s of the input directories/files for the
	 *            map-reduce job.
	 */
	public static void setInputPaths(Job job, Path... inputPaths)
			throws IOException {
		Configuration conf = job.getConfiguration();
		Path path = inputPaths[0].getFileSystem(conf).makeQualified(
				inputPaths[0]);
		StringBuffer str = new StringBuffer(StringUtils.escapeString(path
				.toString()));
		for (int i = 1; i < inputPaths.length; i++) {
			str.append(StringUtils.COMMA_STR);
			path = inputPaths[i].getFileSystem(conf).makeQualified(
					inputPaths[i]);
			str.append(StringUtils.escapeString(path.toString()));
		}
		conf.set(INPUT_DIR, str.toString());
	}

	/**
	 * Add a {@link Path} to the list of inputs for the map-reduce job.
	 * 
	 * @param job
	 *            The {@link Job} to modify
	 * @param path
	 *            {@link Path} to be added to the list of inputs for the
	 *            map-reduce job.
	 */
	public static void addInputPath(Job job, Path path) throws IOException {
		Configuration conf = job.getConfiguration();
		path = path.getFileSystem(conf).makeQualified(path);
		String dirStr = StringUtils.escapeString(path.toString());
		String dirs = conf.get(INPUT_DIR);
		conf.set(INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
	}

	// This method escapes commas in the glob pattern of the given paths.
	private static String[] getPathStrings(String commaSeparatedPaths) {
		int length = commaSeparatedPaths.length();
		int curlyOpen = 0;
		int pathStart = 0;
		boolean globPattern = false;
		List<String> pathStrings = new ArrayList<String>();

		for (int i = 0; i < length; i++) {
			char ch = commaSeparatedPaths.charAt(i);
			switch (ch) {
			case '{': {
				curlyOpen++;
				if (!globPattern) {
					globPattern = true;
				}
				break;
			}
			case '}': {
				curlyOpen--;
				if (curlyOpen == 0 && globPattern) {
					globPattern = false;
				}
				break;
			}
			case ',': {
				if (!globPattern) {
					pathStrings
							.add(commaSeparatedPaths.substring(pathStart, i));
					pathStart = i + 1;
				}
				break;
			}
			default:
				continue; // nothing special to do for this character
			}
		}
		pathStrings.add(commaSeparatedPaths.substring(pathStart, length));

		return pathStrings.toArray(new String[0]);
	}

	/**
	 * Get the list of input {@link Path}s for the map-reduce job.
	 * 
	 * @param context
	 *            The job
	 * @return the list of input {@link Path}s for the map-reduce job.
	 */
	public static Path[] getInputPaths(JobContext context) {
		String dirs = context.getConfiguration().get(INPUT_DIR, "");
		String[] list = StringUtils.split(dirs);
		Path[] result = new Path[list.length];
		for (int i = 0; i < list.length; i++) {
			result[i] = new Path(StringUtils.unEscapeString(list[i]));
		}
		return result;
	}

}
