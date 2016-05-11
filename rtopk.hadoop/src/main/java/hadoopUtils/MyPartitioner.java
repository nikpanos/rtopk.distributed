package hadoopUtils;

import model.MyItem;
import model.MyKey;

import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyKey, MyItem>{

	@Override
	public int getPartition(MyKey key, MyItem value, int numPartitions) {
		return key.getKey();
	}

}
