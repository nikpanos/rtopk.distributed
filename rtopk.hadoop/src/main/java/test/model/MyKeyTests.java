package test.model;

import static org.junit.Assert.assertTrue;
import model.ItemType;
import model.MyKey;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class MyKeyTests {

	@Test
	public void comparableTestKey() {
		MyKey sItem1 = new MyKey(new IntWritable(0),ItemType.S);
		
		MyKey sItem2 = new MyKey(new IntWritable(1), ItemType.S);
		
		assertTrue(sItem2.compareTo(sItem1) == -1);
	}
	
	@Test
	public void comparableTestItemType() {
		MyKey sItem1 = new MyKey(new IntWritable(0), ItemType.S);
		
		MyKey wItem1 = new MyKey(new IntWritable(0), ItemType.W);
		
		MyKey wTopK_Item1 = new MyKey(new IntWritable(0), ItemType.W_InTopK);
		
		assertTrue(sItem1.compareTo(wItem1) == -1);
		assertTrue(wTopK_Item1.compareTo(wItem1) == -1);
		assertTrue(wTopK_Item1.compareTo(sItem1) == -1);
	}
		
	@Test
	public void comparableTestEqualityg(){
		MyKey sItem1 = new MyKey(new IntWritable(0),ItemType.S);
		
		MyKey sItem2 = new MyKey(new IntWritable(0), ItemType.S);
		
		assertTrue(sItem1.compareTo(sItem2) == 0);
	}

}
