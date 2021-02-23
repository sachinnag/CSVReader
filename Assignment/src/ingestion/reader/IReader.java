package ingestion.reader;

import java.util.List;

public interface IReader {

	public long getFileSize();
	public List<String> getHeader();
	public List<List<String>> getRows();
	public int getRowSize();
}
