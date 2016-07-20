package trueGrid;

public class AngleCell {
	private int id;
	private double[] angle1;
	private double[] angle2;
	private double[][] w;
	double[] q;
	double[] scoresQ;
	private int countInAntidominance;
	
	public AngleCell(int id, double[] angle1, double[] angle2) {
		this.id = id;
		this.angle1 = angle1;
		this.angle2 = angle2;
		countInAntidominance = 0;
		
		calculateCordsByAngles();
	}
	
	public AngleCell(int id, double[] angle1, double[] angle2, double[] q) {
		this(id, angle1, angle2);
		this.q = q;
		countInAntidominance = 0;

		calculateScoresQ();
	}
	
	private static double calculateScore(double[] point, double[] w) {
		double result = 0d;
		for (int i = 0; i < w.length; i++) {
			result += point[i] * w[i];
		}
		return result;
	}
	
	private void calculateScoresQ() {
		scoresQ = new double[w.length];
		for (int i = 0; i < w.length; i++) {
			scoresQ[i] = calculateScore(q, w[i]);
		}
	}
	
	public static boolean getBit(int data, int position) {
	   return ((data >> position) & 1) == 1;
	}
	
	public boolean pIsBetterRankedThanQ(float[] p) {
		return pIsBetterRankedThanQ(AngleGrid.convertToDouble(p));
	}
	
	public boolean pIsBetterRankedThanQ(double[] p) {
		double scoreP;
		for (int i = 0; i < w.length; i++) {
			scoreP = calculateScore(p, w[i]);
			if (scoresQ[i] <= scoreP) {
				return false;
			}
		}
		return true;
	}
	
	public boolean qIsBetterRankedThanP(float[] p) {
		return qIsBetterRankedThanP(AngleGrid.convertToDouble(p));
	}
	
	public boolean qIsBetterRankedThanP(double[] p) {
		double scoreP;
		for (int i = 0; i < w.length; i++) {
			scoreP = calculateScore(p, w[i]);
			if (scoreP < scoresQ[i]) {
				return false;
			}
		}
		return true;
	}
	
	public double[] calculateMinMax(double[] a, double[] b, int minmax) {
		double[] result = new double[a.length];
		for (int i = 0; i < a.length; i++) {
			result[i] = (getBit(minmax, i)) ? Math.max(a[i], b[i]) : Math.min(a[i], b[i]);
		}
		return result;
	}

	public void calculateCordsByAngles() {
		int count = (int) Math.pow(2, angle1.length);
		w = new double[count][];
		double[] tmpAngle;
		double r;
		for (int i = 0; i < count; i++) {
			tmpAngle = calculateMinMax(this.angle1, this.angle2, i);
			r = HyperSphere.calculateRByNormalization(tmpAngle);
			this.w[i] = HyperSphere.getCordsByAngles(tmpAngle, r);
		}
	}

	public int getId() {
		return id;
	}

	public double[] getAngle1() {
		return angle1;
	}

	public double[] getAngle2() {
		return angle2;
	}

	public boolean containsWByAngles(double[] a) {
		for (int i = 0; i < a.length; i++) {
			/*if ((a[i] == AngleGrid.semiPI) && ((angle1[i] == a[i]) || (angle2[i] == a[i]))) {
				continue;
			}*/
			if (!isInRange(a[i], angle1[i], angle2[i])) {
				return false;
			}
		}
		return true;
	}

	private boolean isInRange(double x, double a, double b) {
		return (a < b) ? ((a <= x) && (b >= x)) : ((b <= x) && (a >= x));
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < angle1.length; i++) {
			builder.append(angle1[i] + "\t");
		}
		for (int i = 0; i < angle2.length; i++) {
			builder.append(angle2[i]);
			if (i < angle2.length - 1) {
				builder.append("\t");
			}
		}
		return builder.toString();
	}

	public double[][] getW() {
		return w;
	}

	public int getCountInAntidominance() {
		return countInAntidominance;
	}
	
	public int incCountInAntidominance() {
		return ++countInAntidominance;
	}
}
