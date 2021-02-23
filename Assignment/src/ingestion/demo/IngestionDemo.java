package ingestion.demo;

import ingestion.reader.ImportUtil;

public class IngestionDemo {

	public static void main(String[] args) throws Exception {
		
		ImportUtil importUtil = new ImportUtil("Dataset3.csv");
		
		System.out.println("File size in bytes: " + importUtil.getFileSize());
		
		System.out.println("Columns: " + importUtil.getHeaders());
	
		System.out.println("Total rows: " + importUtil.getTotalRows());
		
		importUtil.prettyRows();
	}

}