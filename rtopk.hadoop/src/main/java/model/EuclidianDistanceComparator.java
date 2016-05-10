package model;

import java.util.Comparator;


public class EuclidianDistanceComparator implements Comparator<MyItem>{
	
	private float[] myArray;
	
	public EuclidianDistanceComparator(int dimentionNo, float avgValue) {
		super();
		myArray = new float[dimentionNo];
		for(int i=0;i<dimentionNo;i++){
			myArray[i] = avgValue;
		}
	}

	@Override
	public int compare(MyItem item1, MyItem item2) {
		double score1 = calculateDistance(item1.values, myArray);
		double score2 = calculateDistance(item2.values, myArray);
		if(score1<score2)
			return 1;
		else if(score1>score2)
			return -1;
		else
			return 0;
	}
	
	private double calculateDistance(float[] array1, float[] array2)
    {
        double Sum = 0.0;
        for(int i=0;i<array1.length;i++) {
           Sum = Sum + Math.pow((array1[i]-array2[i]),2.0);
        }
        return Math.sqrt(Sum);
    }
}