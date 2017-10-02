package algorithms;

public class Dominance {

	
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
			if ( p[i] == q[i]) counter1++;
			else if (p[i] < q[i]) counter2++;
			else counter3++;			
		}
		if (counter1==dim) return 0;
		if (counter2+counter1==dim) return 1;
		if (counter3+counter1==dim) return -1;
		return 0;
	}
	
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
			if ( p[i] == q[i]) counter1++;
			else if (p[i] < q[i]) counter2++;
			else counter3++;			
		}
		if (counter1==dim) return -1;
		if (counter2+counter1==dim) return 1;
		if (counter3+counter1==dim) return -1;
		return 0;
	}
	
}
