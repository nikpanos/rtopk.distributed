package hadoopUtils;


import model.MyKey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator{
	
	public MyGroupComparator() {
		super(MyKey.class,true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey key1 = (MyKey) a;
		MyKey key2 = (MyKey) b;
		return key1.getKey().compareTo(key2.getKey());
	}
}
