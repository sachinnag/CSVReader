package ingestion.reader;

public class ReaderFactory {
	
	/** 
	 * The method invokes the creation of object based on the file extension. If the file is valid, 
	 * then it gets parsed and returns back with the parsed object data.
	 * @param fileName - the name of the file to be read and parsed
	 * @return the object containing the parsed data
	 * @throws Exception
	 */
	public IReader getReader(String fileName) throws Exception {
		if(fileName.contains(ParserConstants.CSV_FILE_EXT) && fileName.endsWith(ParserConstants.CSV_FILE_EXT)) {
			return new CSVReaderUtil(fileName);
		}
		else {
			throw new Exception(ParserConstants.FILE_TYPE_ERROR);
		}
	}
}
