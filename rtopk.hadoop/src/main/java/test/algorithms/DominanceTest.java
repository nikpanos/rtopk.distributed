package test.algorithms;

import static org.junit.Assert.*;

import org.junit.Test;

import algorithms.Dominance;

public class DominanceTest {

	@Test
	public void dominateTest(){		
		assertTrue(Dominance.dominate(new float[]{2.0f,2.0f} , new float[]{3.0f,3.0f}) == 1);
		assertTrue(Dominance.dominate(new float[]{3.0f,3.0f} , new float[]{2.0f,2.0f}) == -1);
		assertTrue(Dominance.dominate(new float[]{3.0f,2.0f} , new float[]{2.0f,3.0f}) == 0);
		assertTrue(Dominance.dominate(new float[]{1.0f,1.0f} , new float[]{1.0f,1.0f}) == -1);
	}
	
}
