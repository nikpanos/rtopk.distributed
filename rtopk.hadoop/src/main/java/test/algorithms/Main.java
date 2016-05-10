package test.algorithms;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import algorithms.Brs;
import algorithms.Rta;
import gr.unipi.generators.UniformGenerator;
import model.ItemType;
import model.MyItem;
import model.RTree;

public class Main {
	private static ArrayList<MyItem> generateDatasetS(int size, int dimensions) {
		UniformGenerator generator = new UniformGenerator(dimensions);
		//AntiCorrelatedGenerator generator = new AntiCorrelatedGenerator(4);
		
		ArrayList<MyItem> result = new ArrayList<MyItem>(size);
		for (int i = 0; i < size; i++) {
			 result.add(new MyItem(i, generator.nextPoint(10000000), ItemType.S));
		}
		return result;
	}
	
	private static MyItem[] generateDatasetW(int size, int dimensions) {
		UniformGenerator generator = new UniformGenerator(dimensions);
		//AntiCorrelatedGenerator generator = new AntiCorrelatedGenerator(4);
		
		MyItem[] result = new MyItem[size];
		for (int i = 0; i < size; i++) {
			 result[i] = new MyItem(i, generator.nextNormalizedPointF(), ItemType.W);
		}
		return result;
	}
	/*
	private static MyItem generateQueryPoint(int maxValue, int dimensions) {
		UniformGenerator generator = new UniformGenerator(dimensions);
		
		return new MyItem(generator.nextPoint(maxValue));
	}*/
	
	public static void main(String[] args) {
		int dimensions = 4;
		int k = 10;
		ArrayList<MyItem> datasetS = generateDatasetS(300000, dimensions);
		MyItem[] datasetW = generateDatasetW(30000, dimensions);
		
		//Use the following query for better RTA performance
		MyItem query = new MyItem(new float[] {300000f, 300000f, 300000f, 300000f});
		
		//Or use the following query for better BRS performance
		//MyItem query = new MyItem(new float[] {1f, 1f, 1f, 1f});
		
		System.out.println("Using query:\t" + query.valuesToText().toString());
		
		List<MyItem> resultsRTA = new ArrayList<MyItem>();
		List<MyItem> resultsBRS;
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		System.out.println("Start RTA: " + dateFormat.format(cal.getTime()));
		//RTA Algorithm
		Rta rta = new Rta();// = new Rta();
		for (int i = 0; i < datasetW.length; i++) {
			//rta = new Rta();
			if (rta.isWeightVectorInRtopk(datasetS, datasetW[i], query.getValues(), k)) {
				resultsRTA.add(datasetW[i]);
			}
		}
		
		//resultsRTA = rta.computeRTOPk(datasetS, datasetW, query.getValues(), k);
		cal = Calendar.getInstance();
		System.out.println("End RTA: " + dateFormat.format(cal.getTime()));
		
		
		//BRS Algorithm
		cal = Calendar.getInstance();
		System.out.println("Start build RTree: " + dateFormat.format(cal.getTime()));
		RTree tree = new RTree(dimensions);
		for (int i = 0; i < datasetS.size(); i++) {
			tree.insert(datasetS.get(i));
		}
		cal = Calendar.getInstance();
		System.out.println("End build RTree: " + dateFormat.format(cal.getTime()));
		
		cal = Calendar.getInstance();
		System.out.println("Start BRS: " + dateFormat.format(cal.getTime()));
		Brs brs = new Brs();
		resultsBRS = brs.computeRTOPk(tree, datasetW, query.getValues(), k);
		cal = Calendar.getInstance();
		System.out.println("End BRS: " + dateFormat.format(cal.getTime()));
		
		System.out.println("resultsBRS.size() = " + resultsBRS.size());
		System.out.println("resultsRTA.size() = " + resultsRTA.size());
		
		if (resultsBRS.size() > resultsRTA.size()) {
			resultsBRS.removeAll(resultsRTA);
			System.out.println("NEW resultsBRS.size() = " + resultsBRS.size());
		}
		else {
			resultsRTA.removeAll(resultsBRS);
			System.out.println("NEW resultsBRS.size() = " + resultsRTA.size());
		}
	}
}
