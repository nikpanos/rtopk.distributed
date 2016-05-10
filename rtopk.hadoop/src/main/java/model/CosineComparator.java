package model;

import java.util.Comparator;

public class CosineComparator implements Comparator<MyItem>{

	private float[] myArray;
					
	public CosineComparator(int dimentionNo, float avgValue) {
		super();
		myArray = new float[dimentionNo];
		for(int i=0;i<dimentionNo;i++){
			myArray[i] = avgValue;
		}
	}
	
	@Override
	public int compare(MyItem item1, MyItem item2) {
		double score1 = cosineSimilarity(item1.values, myArray);
		double score2 = cosineSimilarity(item2.values, myArray);
		if(score1>score2)
			return 1;
		else if(score1<score2)
			return -1;
		else
			return 0;
	}
	
	private double cosineSimilarity(float[] vectorA, float[] vectorB) {
	    double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
	    for (int i = 0; i < vectorA.length; i++) {
	        dotProduct += vectorA[i] * vectorB[i];
	        normA += Math.pow(vectorA[i], 2);
	        normB += Math.pow(vectorB[i], 2);
	    }   
	    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	}
}