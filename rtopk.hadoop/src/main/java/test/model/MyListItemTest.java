package test.model;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import model.Cell_W;
import model.ItemType;
import model.MyItem;
import model.MyListItem;

import org.junit.Test;

import algorithms.Functions;

public class MyListItemTest {

	@Test
	public void addTest(){
		Cell_W cellW = new Cell_W(0, 2);
		cellW.setLowerBound(new float[]{0.0f,0.25f});
		cellW.setUpperBound(new float[]{0.25f,0.5f});
		int k = 5;
		MyListItem listItem = new MyListItem(cellW, k);
		
		ArrayList<MyItem> s = new ArrayList<MyItem>(10000);
		ArrayList<MyItem> results = new ArrayList<MyItem>();
		
		for(int i=0;i<10000;i++){
			MyItem item = new MyItem(i, 
					new float[]{(float) (Math.random() * 1000 + 1),(float) (Math.random() * 1000 + 1)},
				ItemType.S);
			s.add(item);
			if(listItem.add(item))
				results.add(item);
		}
		
		ArrayList<MyItem> w = new ArrayList<MyItem>(10000);
		
		for(int i=0;i<10000;i++){
			float xDimension = (float) (Math.random()*0.25);
			float yDimension = 1 - xDimension;
			assertTrue(xDimension<=0.25);
			assertTrue(xDimension>=0);
			assertTrue(xDimension<=0.75);
			assertTrue(yDimension>=0.5);
			assertTrue(xDimension+yDimension==1);
			w.add(new MyItem(i, 
					new float[]{
				xDimension,
				yDimension},
				ItemType.W));
		}
		
		for (MyItem wItem : w) {
			ComparatorForTopK comparator = new ComparatorForTopK(wItem);
			Collections.sort(results,comparator);
			Collections.sort(s,comparator);
			for(int i=0;i<k;i++)
				assertEquals(s.get(i),results.get(i));
		}
	}
	
	private class ComparatorForTopK implements Comparator<MyItem>{

		private MyItem w;
		
		public ComparatorForTopK(MyItem w) {
			super();
			this.w = w;
		}
		
		@Override
		public int compare(MyItem s1, MyItem s2) {
			if(Functions.calculateScore(w, s1)<Functions.calculateScore(w, s2))
				return -1;
			else if(Functions.calculateScore(w, s1)>Functions.calculateScore(w, s2))
				return 1;
			else
				return 0;
		}
		
		
		
	}
}
