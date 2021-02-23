package ingestion.reader;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ImportUtil {

	private static Logger logger = Logger.getLogger(ImportUtil.class.getName());
	
	private IReader reader;
	
	/**
	 * Constructor initializes and invokes the reading of the file.
	 * @param fileName - the name of the file to be read and parsed
	 */
	public ImportUtil(String fileName) {
		readFile(fileName);
	}
	
	/**
	 * The ReaderFactory invokes the reading and parsing of the file and returns back the Reader object for manipulation.
	 * @param fileName - the name of the file to be read and parsed
	 */
	public void readFile(String fileName) {
		ReaderFactory readerFactory = new ReaderFactory();
		try {
			reader = readerFactory.getReader(fileName);
		}
		catch(Exception exc) {
			logger.log(Level.SEVERE, exc.getMessage(), exc);
		}
	}
	
	/** Gets the file size in bytes.
	 * @return
	 */
	public long getFileSize() {
		return reader.getFileSize();
	}

	/** Gets the headers of the file.
	 * @return
	 */
	public List<String> getHeaders() {
		return reader.getHeader();
	}

	/** Gets the total number of rows.
	 * @return
	 */
	public int getTotalRows() {
		return reader.getRowSize();
	}
	
	/** Gets all the rows of the file excluding the headers
	 * @return
	 */
	public List<List<String>> getAllRows() {
		return reader.getRows();
	}
	
	/**
	 * Gets all the rows in pretty format.
	 */
	public void prettyRows() {
		reader.getRows().stream().forEach(System.out::println);
	}
}
