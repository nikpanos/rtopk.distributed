package algorithms;

import model.MyItem;


public class Functions {
	
	public static float calculateScore(MyItem w, MyItem p) {
		return calculateScore(w.getFields(), p.getFields());
	}
	
	public static float calculateScore(float[] w, float[] p) {
		if (w.length != p.length)
			throw new IllegalArgumentException("Must have the same dimentions in order to calculate f!!!");
		
		float score = 0;

		for (int i = 0; i < w.length; i++) {
			score += w[i] * p[i];
		}

		return score;
	}
	
		
	/**
	 * <h1>Calculate the score</h1>
	 * 
	 * This method calculate the score of a product p for a user's preferences W.
	 * The smaller the score the better (is better for the user's preferences).
	 * 
	 * @param W
	 *            user's preferences
	 * @param p
	 *            product
	 * @return score
	 */
	public static float calculateScore(MyItem W, float[] p) {

		if (W.values.length != p.length)
			throw new IllegalArgumentException(
					"Must have the same dimentions in order to calculate f!!!");

		float score = 0;

		for (int i = 0; i < W.values.length; i++) {
			score += W.values[i] * p[i];
		}

		return score;
	}
	
	/**
	 * <h1>Calculate the score</h1>
	 * 
	 * This method calculate the score of a product p for a user's preferences W.
	 * The smaller the score the better (is better for the user's preferences).
	 * 
	 * @param W
	 *            user's preferences
	 * @param p
	 *            product
	 * @return score
	 */
	public static double calculateScore(Double[] W, float[] p) {

		if (W.length != p.length)
			throw new IllegalArgumentException(
					"Must have the same dimentions in order to calculate f!!!");

		double score = 0;

		for (int i = 0; i < W.length; i++) {
			score += W[i] * p[i];
		}

		return score;
	}
	
	/**
	 * <h1>Calculate the score</h1>
	 * 
	 * This method calculate the score of a product p for a user's preferences W.
	 * The smaller the score the better (is better for the user's preferences).
	 * 
	 * @param W
	 *            user's preferences
	 * @param p
	 *            product
	 * @return score
	 */
	public static double calculateScore(float[] W, MyItem p) {

		if (W.length != p.values.length)
			throw new IllegalArgumentException(
					"Must have the same dimentions in order to calculate f!!!");

		double score = 0;

		for (int i = 0; i < W.length; i++) {
			score += W[i] * p.values[i];
		}

		return score;
	}
	
	/**
	 * <h1>Calculate the score</h1>
	 * 
	 * This method calculate the score of a product p for a user's preferences W.
	 * The smaller the score the better (is better for the user's preferences).
	 * 
	 * @param W
	 *            user's preferences
	 * @param p
	 *            product
	 * @return score
	 */
	/*public static double calculateScore(MyItem W, MyItem p) {

		if (W.values.length != p.values.length)
			throw new IllegalArgumentException(
					"Must have the same dimentions in order to calculate f!!!");

		double score = 0;

		for (int i = 0; i < W.values.length; i++) {
			score += W.values[i].get() * p.values[i].get();
		}

		return score;
	}*/
}
