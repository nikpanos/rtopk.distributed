package algorithms;

public class Dominance {

	/**
	 * 
	 * 1  - Tο p κυριαρχεί του q<br/>
	 * -1 - Tο q κυριαρχεί του p<br/>
	 * 0  - Tα q και p είναι ισόπαλα<br/>
	 * 
	 * @param p 
	 * @param q
	 * @return
	 */
	public static int dominate(float p[],float q[])
	{
		int dim = p.length;
		if ( q.length != dim)
			throw new IllegalArgumentException("Dimension out of range!");

		int counter1 = 0;
		int counter2 = 0;
		int counter3 = 0;
		for (int i=0;i<dim;i++)
		{			 
			if ( p[i] == q[i]) counter1++;// ισόπαλα
			else if (p[i] < q[i]) counter2++;// + για το p
			else counter3++;			// + για το q
		}
		if (counter1==dim) return 0;
		if (counter2+counter1==dim) return 1;
		if (counter3+counter1==dim) return -1;
		return 0;
	}
	
	/**
	 * 
	 * 1  - Tο p κυριαρχεί του q<br/>
	 * -1 - Tο q κυριαρχεί του p<br/>
	 * -1  - Tα q και p είναι ισόπαλα<br/>
	 * 0   - Αν δεν μπορούμε να βγάλουμε συμπέρασμα
	 * @param p 
	 * @param q
	 * @return
	 */
	public static int dominateBoostQuery(float p[],float q[])
	{
		int dim = p.length;
		if ( q.length != dim)
			throw new IllegalArgumentException("Dimension out of range!");

		int counter1 = 0;
		int counter2 = 0;
		int counter3 = 0;
		for (int i=0;i<dim;i++)
		{			 
			if ( p[i] == q[i]) counter1++;// ισόπαλα
			else if (p[i] < q[i]) counter2++;// + για το p
			else counter3++;			// + για το q
		}
		if (counter1==dim) return -1;
		if (counter2+counter1==dim) return 1;
		if (counter3+counter1==dim) return -1;
		return 0;
	}
	
}
