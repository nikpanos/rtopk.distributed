package algorithms;

import grids.gridsS.GridS;
import grids.gridsW.GridW;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import model.Cell_S;
import model.Cell_W;
import model.MyItem;

public class FileParser {

	/**
	 * <h1>Parse line of dataset S or W</h1>
	 * 
	 * This method parse a line of this type:<br/>
	 * <table>
	 * <tbody>
	 * <tr>
	 * <th>id</th>
	 * <th>dim1</th>
	 * <th>dim2</th>
	 * <th>...</th>
	 * <th>dimN</th>
	 * </tr>
	 * <tr>
	 * <th>0</th>
	 * <th>21.0</th>
	 * <th>23.4</th>
	 * <th>...</th>
	 * <th>3.0</th>
	 * </tr>
	 * <tr>
	 * <th>1</th>
	 * <th>0.34</th>
	 * <th>21.2</th>
	 * <th>...</th>
	 * <th>22.4</th>
	 * </tr>
	 * </tbody>
	 * </table>
	 * 
	 * 
	 * @param line A line of dataset S or W
	 * @return An MyItem object of given line
	 */
	public static MyItem parseDatasetElement(String line) {
		// Break the line by whitespace (" ")
		StringTokenizer tokenizer = new StringTokenizer(line);

		// Read the element's id which found in the first column of the line
		long id = Long.parseLong(tokenizer.nextToken());

		// Create a DoubleWritalbe array in order to store the value of each dimension
		float[] currentItemValues = new float[tokenizer
				.countTokens()];

		// Initialize a counter, in order to know the current dimension
		int i = 0;
		// While the element has another token (dimension),
		// repeat...
		while (tokenizer.hasMoreTokens()) {
			// Set the value of dimension i to the i position of array currentItemValues
			currentItemValues[i] = Float.parseFloat(tokenizer.nextToken());
			// Increase counter value by 1
			i++;
		}

		// Return an MyItem object
		return new MyItem(id, currentItemValues);
	}
	
	/**
	 * <h1>Create a Grid for dataset S</h1>
	 * 
	 * This method read a file that contains a grid for dataset S and create an object for this.
	 * <br/>
	 * The file must have this format (LB-lower bound,UP-upper bound):
	 * <table>
	 * <tbody>
	 * <tr>
	 * <th>id</th>
	 * <th>Count</th>
	 * <th>LB_dim1</th>
	 * <th>LB_dim2</th>
	 * <th>...</th>
	 * <th>LB_dimN</th>
	 * <th>UB_dim1</th>
	 * <th>UB_dim2</th>
	 * <th>...</th>
	 * <th>UB_dimN</th>
	 * </tr>
	 * <tr>
	 * <th>0</th>
	 * <th>5</th>
	 * <th>21.0</th>
	 * <th>23.4</th>
	 * <th>...</th>
	 * <th>3.0</th>
	 * <th>27.0</th>
	 * <th>31.2</th>
	 * <th>...</th>
	 * <th>7.3</th>
	 * </tr>
	 * <tr>
	 * <th>1</th>
	 * <th>8</th>
	 * <th>0.34</th>
	 * <th>21.2</th>
	 * <th>...</th>
	 * <th>22.4</th>
	 * <th>3.5</th>
	 * <th>31.2</th>
	 * <th>...</th>
	 * <th>27.5</th>
	 * </tr>
	 * </tbody>
	 * </table>
	 * 
	 * 
	 * @param path the path of the file that contains the Grid
	 * @param grid A Grid object
	 * @throws IOException
	 */
	public static void parseGridSFile(URI path, GridS grid) throws IOException{
				
		File gridFile = new File(path.toString());
		
		BufferedReader in = new BufferedReader(new FileReader(gridFile));
		String line;
		StringTokenizer tokenizer;
				
		// while file has more lines, repeat...
		while((line = in.readLine()) != null)
		{
			
			// Break the line by whitespace (" ")
			tokenizer = new StringTokenizer(line);
			
			// Calculate the dimensions number 
			// (-2 because 1 column is id and 1 the count, 
			// /2 because keep Lower and Upper bound)
			int dimensionsNumber = (tokenizer.countTokens()-2)/2;
			
			// Read the cell's id which found in the first column of the line
			int id = Integer.parseInt(tokenizer.nextToken());
			// Read the cell's count which found in the second column of the line
			int count = Integer.parseInt(tokenizer.nextToken());
			
			//Initialize a cell
			Cell_S cell = new Cell_S(id, count, dimensionsNumber);
			
			// Initialize a counter, in order to know the current dimension
			int i = 0;
			
			// While the element has another token (dimension),
			// repeat...
			while (tokenizer.hasMoreTokens()) {				
				
				// if read the lower bound
				if(i<dimensionsNumber)
					cell.getLowerBound()[i] = Float.parseFloat(tokenizer.nextToken());
				// else if read the upper bound
				else
					cell.getUpperBound()[i-dimensionsNumber] = Float.parseFloat(tokenizer.nextToken());
				
				// increase the counter by 1
				i++;
			}
			
			// add the current cell to grid
			grid.add(cell);
			
		}
		
		// Close the BufferReader
		in.close();
		
	}
	
	/**
	 * <h1>Create a Grid for dataset W</h1>
	 * 
	 * This method read a file that contains a grid for dataset W and create an object for this.
	 * <br/>
	 * The file must have this format (LB-lower bound,UP-upper bound):
	 * <table>
	 * <tbody>
	 * <tr>
	 * <th>id</th>
	 * <th>LB_dim1</th>
	 * <th>LB_dim2</th>
	 * <th>...</th>
	 * <th>LB_dimN</th>
	 * <th>UB_dim1</th>
	 * <th>UB_dim2</th>
	 * <th>...</th>
	 * <th>UB_dimN</th>
	 * </tr>
	 * <tr>
	 * <th>0</th>
	 * <th>0.0</th>
	 * <th>0.75</th>
	 * <th>...</th>
	 * <th>xyz</th>
	 * <th>0.25</th>
	 * <th>1.0</th>
	 * <th>...</th>
	 * <th>xyz</th>
	 * </tr>
	 * <tr>
	 * <th>1</th>
	 * <th>0.25</th>
	 * <th>0.5</th>
	 * <th>...</th>
	 * <th>xyz</th>
	 * <th>0.5</th>
	 * <th>0.75</th>
	 * <th>...</th>
	 * <th>xyz</th>
	 * </tr>
	 * </tbody>
	 * </table>
	 * 
	 * 
	 * @param path the path of the file that contains the Grid
	 * @param grid A Grid object
	 * @throws IOException
	 */
	public static void parseGridWFullDimentionFile(URI path, GridW grid) throws IOException{
				
		File gridFile = new File(path.toString());
		BufferedReader in = new BufferedReader(new FileReader(gridFile));
		String line;
		StringTokenizer tokenizer;
		int id = 0;
		// while file has more lines, repeat...
		while((line = in.readLine()) != null)
		{
			
			// Break the line by whitespace (" ")
			tokenizer = new StringTokenizer(line);
			
			// Calculate the dimensions number 
			// (-1 because 1 column is id, 
			// /2 because keep Lower and Upper bound)
			int dimensionsNumber = (tokenizer.countTokens()-1)/2;
			
			// Read the cell's id which found in the first column of the line
			@SuppressWarnings("unused")
			int idn = Integer.parseInt(tokenizer.nextToken());
			
			
			//Initialize a cell
			Cell_W cell = new Cell_W(id,dimensionsNumber);
			
			// Initialize a counter, in order to know the current dimension
			int i = 0;
			
			// While the element has another token (dimension),
			// repeat...
			while (tokenizer.hasMoreTokens()) {				
				
				// if read the lower bound
				if(i<dimensionsNumber)
					cell.getLowerBound()[i] = Float.parseFloat(tokenizer.nextToken());
				// else if read the upper bound
				else
					cell.getUpperBound()[i-dimensionsNumber] = Float.parseFloat(tokenizer.nextToken());
				
				// increase the counter by 1
				i++;
			}
			
			// add the current cell to grid
			grid.add(cell);
			
			id++;
		}
		
		// Close the BufferReader
		in.close();
		
	}
	
	/**
	 * <h1>Create a Grid for dataset W (d-1)</h1>
	 * 
	 * This method read a file that contains a grid for dataset W (d-1 dimensions) and create an object for this.
	 * <br/>
	 * The file must have this format (LB-lower bound,UP-upper bound):
	 * <table>
	 * <tbody>
	 * <tr>
	 * <th>id</th>
	 * <th>LB_dim1</th>
	 * <th>LB_dim2</th>
	 * <th>...</th>
	 * <th>LB_dimN-1</th>
	 * <th>UB_dim1</th>
	 * <th>UB_dim2</th>
	 * <th>...</th>
	 * <th>UB_dimN-1</th>
	 * </tr>
	 * <tr>
	 * <th>0</th>
	 * <th>0.0</th>
	 * <th>0.1</th>
	 * <th>...</th>
	 * <th>xyz</th>
	 * <th>0.25</th>
	 * <th>0.75</th>
	 * <th>...</th>
	 * <th>xyz</th>
	 * </tr>
	 * <tr>
	 * <th>1</th>
	 * <th>0.25</th>
	 * <th>0.75</th>
	 * <th>...</th>
	 * <th>xyz</th>
	 * <th>0.5</th>
	 * <th>0.5</th>
	 * <th>...</th>
	 * <th>xyz</th>
	 * </tr>
	 * </tbody>
	 * </table>
	 * 
	 * @param path the path of the file that contains the Grid
	 * @throws IOException
	 */
	public static void parseGridWNotFullDimentionFile(URI path,GridW grid) throws IOException{
		
		File gridFile = new File(path.toString());
		BufferedReader in = new BufferedReader(new FileReader(gridFile));
		String line;
		StringTokenizer tokenizer;
		int id = 0;
		// while file has more lines, repeat...
		while((line = in.readLine()) != null)
		{
			
			// Break the line by whitespace (" ")
			tokenizer = new StringTokenizer(line);
			
			// Calculate the dimensions number 
			// (-1 because 1 column is id, 
			// +2 because 2*(-1 missing dimensions) 
			// /2 because keep Lower and Upper bound)
			int dimensionsNumber = (tokenizer.countTokens()-1+2)/2;
			
			// Read the cell's id which found in the first column of the line
			@SuppressWarnings("unused")
			int idn = Integer.parseInt(tokenizer.nextToken());
			
			//Initialize a cell
			Cell_W cell = new Cell_W(id,dimensionsNumber);
			
			// Initialize a counter, in order to know the current dimension
			int i = 0;
			
			float maxUp = (float) 1.0;
			float maxLo = (float) 1.0;
			
			// While the element has another token (dimension),
			// repeat...
			while (tokenizer.hasMoreTokens()) {				
				
				// if read the lower bound
				if(i<dimensionsNumber-1){
					cell.getLowerBound()[i] = Float.parseFloat(tokenizer.nextToken());
					maxLo -= cell.getLowerBound()[i];
				}
				else if(i==dimensionsNumber-1){
					cell.getLowerBound()[i] = maxLo;                   
				}
				// else if read the upper bound
				else if (i-dimensionsNumber < dimensionsNumber-1){
					cell.getUpperBound()[i-dimensionsNumber] = Float.parseFloat(tokenizer.nextToken());
					maxUp -= cell.getUpperBound()[i-dimensionsNumber];
				}
				
				// increase the counter by 1
				i++;
			}
			cell.getUpperBound()[i-dimensionsNumber] = maxUp;
			
			// add the current cell to grid
			grid.add(cell);
			
			id++;
		}
		
		// Close the BufferReader
		in.close();
	}

}
