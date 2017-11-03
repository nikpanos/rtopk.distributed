package hadoopUtils;

import java.util.Comparator;

import model.MyItem;

/**
 * � ����� ���� ����� ���� Comparator ��� ��������� ��� ����������� MyItem ������� �� �� query.
 * @author George
 *
 */
public class MyComparatorForW implements Comparator<MyItem>{

	private static double[] myArray;
	
	public MyComparatorForW(int dimentionNo, double avgValue) {
		myArray = new double[dimentionNo];
		for(int i=0;i<=dimentionNo-1;i++){
			myArray[i] = avgValue;
		}
	}
	
	@Override
	public int compare(MyItem item1, MyItem item2) {
		if (item1.getCosine() < item1.getCosine()) {
			return -1;
		}
		else if (item1.getCosine() < item1.getCosine()) {
			return 1;
		}
		else {
			return 0;
		}
		/*
		double score1 = cosineSimilarity(item1.values, myArray);
		double score2 = cosineSimilarity(item2.values, myArray);
		if(score1<score2)
			return 1;
		else if(score1>score2)
			return -1;
		else
			return 0;*/
	}
	/*
	private static double cosineSimilarity(double[] vectorA, double[] vectorB) {
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
	*/
}
