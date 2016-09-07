package core.dataContainer;

/*
 * represents an abstraction for data containers that will be used to store data from sources
 * like(hdfs, elastic search, jsonfiles,...)
 */
public abstract class AbstractContainer {
 
	/*
  * writer connection
  */
	public abstract void WriterConnect();
	
	/*
	 * writer Disconnect 
	 */
	public abstract void WriterDisconnect();
	
	/*
	 * Reader Connect
	 */
	public abstract void ReaderConnect();
	
	/*
	 * Reader disconnect
	 */
	public abstract void ReaderDisconnect();
	
	/*
	 * Write Operation
	 */
	public abstract void Write(String data,String id);
	
	public abstract void Write(String data);
	
	/*
	 * read operation
	 */
	public abstract String Read();
}

