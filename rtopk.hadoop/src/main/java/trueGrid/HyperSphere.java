package trueGrid;

public class HyperSphere {
	public static double calculateRByNormalization(double[] angles) {
		double result = 0f;
		for (int i = 0; i <= angles.length; i++) {
			result += calculateAdvisor(angles, i);
		}
		return 1 / result;
	}
	
	public static double calculateRByCords(double[] w) {
		double result = 0f;
		for (int i = 0; i < w.length; i++) {
			result += Math.pow(w[i], 2);
		}

		return Math.sqrt(result);
	}
	
	
	private static double calculateAdvisor(double[] angles, int dimension) {
		double result = 1f;
		/*
		if (angles.length == 2) {
			if (dimension == 0) {
				result = Math.sin(angles[1]) * Math.cos(angles[0]);
			}
			else if (dimension == 1) {
				result = Math.sin(angles[1]) * Math.sin(angles[0]);
			}
			else {
				result = Math.cos(angles[1]);
			}
		}
		else {*/
			for (int i = 0; i <= dimension; i++) {
				if ((i == angles.length - 1) && (dimension == angles.length)) {
					result *= Math.sin(angles[i]);
					break;
				} else if (i == dimension) {
					result *= Math.cos(angles[i]);
				} else {
					result *= Math.sin(angles[i]);
				}
			}
		//}
		return result;
	}
	
	private static double calculateAngleByCords(double[] w, int dimension) {
		double tmp = 0;
		for (int i = dimension; i < w.length; i++) {
			tmp += Math.pow(w[i], 2);
		}

		if (tmp == 0) {
			return 0f;
		}

		tmp = w[dimension] / Math.sqrt(tmp);
		return Math.acos(tmp);
	}
	
	public static double[] getAnglesByCords(double[] w) {
		double[] result = new double[w.length - 1];

		for (int i = 0; i < result.length; i++) {
			result[i] = calculateAngleByCords(w, i);
		}

		return result;
	}
	
	public static double[] getCordsByAngles(double[] angles, double r) {
		double[] result = new double[angles.length + 1];
		
		/*if (angles.length == 2) {
			result[0] = r * Math.sin(angles[1]) * Math.cos(angles[0]);
			result[1] = r * Math.sin(angles[1]) * Math.sin(angles[0]);
			result[2] = r * Math.cos(angles[1]);
		}
		else {*/
			for (int i = 0; i < result.length; i++) {
				result[i] = Math.abs(calculateAdvisor(angles, i) * r);
			}
		//}
		return result;
	}
}
