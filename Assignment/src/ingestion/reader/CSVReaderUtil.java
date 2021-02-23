package ingestion.reader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class CSVReaderUtil implements IReader {

	private static Logger logger = Logger.getLogger(CSVReaderUtil.class.getName());
	
	private long fileSize; 
	private List<String> headerData = new ArrayList<>();
	private List<List<String>> rowData = new ArrayList<>();
	
	/**
	 * Constructor initializes the reading of the CSV File at the time of object creation and initialization.
	 * @param fileName - the name of the file to be read and parsed
	 * @throws Exception
	 */
	public CSVReaderUtil(String fileName) throws Exception {
		initReader(fileName);
	}
	
	/**
	 * Driver method to get the CSV File from the file-name and invoke the reading of the file.
	 * @param fileName - the name of the file to be read and parsed
	 * @throws Exception
	 */
	public void initReader(String fileName) throws Exception {
		File csvFile = new File(ParserConstants.FILE_PATH + fileName);
		readCsvFile(csvFile, ParserConstants.METADATA_LINE, ParserConstants.SEPARATOR);
	}
	
	/**
	 * The method read the CSV file line by line, parses and stores the data.
	 * @param csvFile - the CSV file to be read and parsed
	 * @param metaDataLine - the row index for the header/column
	 * @param separator - the separating the data in the line
	 * @throws Exception
	 */
	public void readCsvFile(File csvFile, int metaDataLine, String separator) throws Exception {
		
		int lineIndex = 0;
		String line;
		
		try(BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
			
			// Gets the File length in bytes
			this.fileSize = csvFile.length();
									
			while((line = br.readLine()) != null) {
				
				// Gets the header/column names
				if(lineIndex == metaDataLine) {
					headerData.addAll(Arrays.asList(line.split(separator)));
					lineIndex++;
					continue;
				}
				
				// Gets the row data line by line
				parseLine(line, separator);
			}
		}
		catch(FileNotFoundException exc) {
			logger.log(Level.SEVERE, ParserConstants.FILE_ERROR);
		}
		catch(Exception exc) {
			logger.log(Level.SEVERE, exc.getMessage(), exc);
		}
	}
	
	/**
	 * Parses the line, separates the values using the separator and stores it row by row.
	 * @param line
	 * @param separator
	 */
	private void parseLine(String line, String separator) {
		if(!line.isEmpty()) {
			List<String> rowValueList = Arrays.stream(line.split(separator))
											.map(elem -> elem.trim())
											.collect(Collectors.toList());
			rowData.add(rowValueList);
		}
	}

	/**
	 * Returns the size of the file in bytes.
	 */
	@Override
	public long getFileSize() {
		return this.fileSize;
	}

	/**
	 * Returns the headers of the file.
	 */
	@Override
	public List<String> getHeader() {
		return this.headerData;
	}

	/**
	 * Returns all the rows of the file
	 */
	@Override
	public List<List<String>> getRows() {
		return this.rowData;
	}
	
	/**
	 * Returns the total number of rows.
	 */
	@Override
	public int getRowSize() {
		return this.rowData.size();
	}
	
}
