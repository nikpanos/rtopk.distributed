package test.hadoopUtils;

import static org.junit.Assert.*;
import hadoopUtils.MyPartitioner;
import model.ItemType;
import model.MyKey;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class MyPartionerTest {

	@Test
	public void getPartitionTest() {
		MyPartitioner partitioner = new MyPartitioner();
		MyKey key1 = new MyKey(new IntWritable(1), ItemType.S);
		MyKey key2 = new MyKey(new IntWritable(1), ItemType.W);
		MyKey key3 = new MyKey(new IntWritable(2), ItemType.S);
		MyKey key4 = new MyKey(new IntWritable(100), ItemType.S);
		MyKey key5 = new MyKey(new IntWritable(100), ItemType.W);
		int p1 = partitioner.getPartition(key1, null, 10);
		int p2 = partitioner.getPartition(key2, null, 10);
		int p3 = partitioner.getPartition(key3, null, 10);
		int p4 = partitioner.getPartition(key4, null, 10);
		int p5 = partitioner.getPartition(key5, null, 10);
		assertEquals(p1, p2);
		assertNotEquals(p1, p3);
		assertNotEquals(p1, p4);
		assertNotEquals(p3, p4);
		assertEquals(p4, p5);
		assertNotSame(key1, key2);
		assertNotSame(key4, key5);
	}

}
