package trueGrid;

import java.io.FileWriter;
import java.io.IOException;

public class AngleGrid {
	private AngleCell[] cells;
	private int dimensions;
	private int segments;
	private double segmentSize;
	public static final double semiPI = Math.PI / 2;
	private double[] q;
	
	public AngleGrid(int segments, int dimensions) {
		int cellCount = (int) Math.pow(segments, dimensions - 1);
		this.dimensions = dimensions;
		this.segments = segments;
		this.segmentSize = semiPI / (double)segments;
		
		cells = new AngleCell[cellCount];
		double[] a1, a2;
		for (int i = 0; i < cellCount; i++) {
			a1 = getCellOrigin(i, semiPI, segments, dimensions - 1);
			a2 = getCellConclusion(i, semiPI, segments, dimensions - 1);
			//a1 = getCellOrigin(i, Math.PI / 2, segments, dimensions - 1);
			//a2 = getCellConclusion(i, Math.PI / 2, segments, dimensions - 1);
			cells[i] = new AngleCell(i, a1, a2);
		}
	}
	
	public AngleGrid(int segments, float[] q) {
		int cellCount = (int) Math.pow(segments, q.length - 1);
		this.dimensions = q.length;
		this.segments = segments;
		this.segmentSize = semiPI / (double)segments;
		this.q = convertToDouble(q);
		
		cells = new AngleCell[cellCount];
		double[] a1, a2;
		//double increment = 0.0001d;
		for (int i = 0; i < cellCount; i++) {
			a1 = getCellOrigin(i, semiPI, segments, dimensions - 1);
			a2 = getCellConclusion(i, semiPI, segments, dimensions - 1);
			//a1 = getCellOrigin(i, Math.PI / 2, segments, dimensions - 1);
			//a2 = getCellConclusion(i, Math.PI / 2, segments, dimensions - 1);
			cells[i] = new AngleCell(i, a1, a2, this.q);
		}
	}
	
	public AngleGrid(String[] file, float[] q) {
		String[] headers = file[0].split("\t");
		int cellCount = Integer.parseInt(headers[0]);
		this.dimensions = Integer.parseInt(headers[1]);
		this.segments = Integer.parseInt(headers[2]);
		this.segmentSize = semiPI / (double)segments;
		this.q = convertToDouble(q);
		
		cells = new AngleCell[cellCount];
		double[] a1, a2;
		String[] lineTokens;
		for (int i = 0; i < cellCount; i++) {
			lineTokens = file[i + 1].split("\t");
			a1 = new double[dimensions - 1];
			for (int j = 0; j < a1.length; j++) {
				a1[j] = Double.parseDouble(lineTokens[j + 1]);
			}
			a2 = new double[dimensions - 1];
			for (int j = 0; j < a2.length; j++) {
				a2[j] = Double.parseDouble(lineTokens[j + dimensions]);
			}
			cells[i] = new AngleCell(i, a1, a2, this.q);
		}
	}
	
	public void saveToFile(String filename) throws IOException {
		FileWriter wr = new FileWriter(filename);
		try {
			wr.append(cells.length + "\t" + dimensions + "\t" + segments + "\n");
			for (int i = 0; i < cells.length; i++) {
				wr.append(i + "\t" + cells[i].toString() + "\n");
			}
		}
		finally {
			wr.flush();
			wr.close();
		}
	}
	
	private double[] getCellOrigin(long cellId, double max_value, int segments, int dimensions) {
		double step = ((double) max_value) / (double) segments;
		
		int[] cellDescriptors = new int[dimensions];
		for (int i = dimensions - 1; i >= 0; i--) {
			cellDescriptors[i] = (int) (cellId / Math.pow(segments, i));
			cellId -= cellDescriptors[i] * Math.pow(segments, i);
		}
		
		double[] fields = new double[dimensions];
		for (int i = 0; i < dimensions; i++) {
			fields[i] = (((double)cellDescriptors[i]) * step);
		}
		return fields;
	}
	
	private double[] getCellConclusion(long cellId, double max_value, int segments, int dimensions) {
		double step = ((double) max_value) / (double) segments;
		double[] result = getCellOrigin(cellId, max_value, segments, dimensions);
		for (int i = 0; i < result.length; i++) {
			result[i] += step;
		}
		return result;
	}
	/*
	private static double[] convertToRadians(double[] x) {
		double[] result = new double[x.length];
		for (int i = 0; i < x.length; i++) {
			result[i] = Math.toRadians(x[i]);
		}
		return result;
	}
	*/
	/*public static double[] round(double value[], int places) {
	    if (places < 0) throw new IllegalArgumentException();
	    double[] result = new double[value.length];
	    for (int i = 0; i < result.length; i++) {
	    	if (value[i] == Math.PI / 2) {
	    		result[i] = value[i];
	    		continue;
	    	}
		    BigDecimal bd = new BigDecimal(value[i]);
		    bd = bd.setScale(places, RoundingMode.HALF_EVEN);
		    result[i] = bd.doubleValue();
	    }
	    return result;
	}*/
	
	public static double[] convertToDouble(float[] x) {
		double[] result = new double[x.length];
		for (int i = 0; i < x.length; i++) {
			result[i] = (double) x[i];
		}
		return result;
	}
	
	public int getCellIdByCords(float[] w) {
		double[] angles = HyperSphere.getAnglesByCords(convertToDouble(w));
		return getCellIdByAngles(angles);
	}
	
	public int getCellIdByAngles(double[] angles) {
		/*for (int i = 0; i < cells.length; i++) {
			if (cells[i].containsWByAngles(angles)) {
				return i;
			}
		}
		return -1;*/
		int result = 0;
		int cellDescriptor;
		for (int i = 0; i < angles.length; i++) {
			if (angles[i] == semiPI) {
				cellDescriptor = segments - 1;
			}
			else {
				cellDescriptor = (int) Math.floor(angles[i] / segmentSize);
			}
			result += cellDescriptor * Math.pow(segments, i);
		}
		return result;
	}

	public AngleCell[] getCells() {
		return cells;
	}

	public int getDimensions() {
		return dimensions;
	}

	public int getSegments() {
		return segments;
	}
}
