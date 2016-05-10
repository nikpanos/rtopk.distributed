package test.model;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import model.ItemType;
import model.MyItem;
import model.RTree;

import org.junit.Test;

public class RTreeTest {
	
	@Test
	public void testRTree(){
		
		ArrayList<MyItem> s = new ArrayList<MyItem>(1000000);
		RTree tree = new RTree(2);
		
		for(int i=0;i<1000000;i++){
			MyItem item = new MyItem(i, 
					new float[]{
				(float) (Math.random() * 1000 + 1),
				(float) (Math.random() * 1000 + 1)},
				ItemType.S);
			s.add(item);
			tree.insert(item);
		}
		
		assertEquals(tree.size(),s.size());
	}
	
}
