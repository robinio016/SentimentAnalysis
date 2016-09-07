package core.dataContainer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import core.base.Properties;
import scala.Int;

public class HdfsContainer extends AbstractContainer {

	//-------------------------------------------------------------
	//			Constructors
	//-------------------------------------------------------------
	
	/*
	 * setdefault configurations
	 */
	public HdfsContainer() throws IOException{
		this.conf=new Configuration();
		this.conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		this.conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		this.fs = FileSystem.get(conf);
	}
	
	//---------------------------------------------------------------
	//			Implements
	//---------------------------------------------------------------
	/*
	 * set path to save folder
	 */
	@Override
	public void WriterConnect() {
		
		this.keywords = Properties.getString("twitter.keyword","spark");
		this.path=Properties.getString("spark.saveJsonTo","spark");
		try {
			mkdir(this.path, this.keywords);
		} catch (IOException e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
		}
	}

	/*
	 * close fileSystem
	 */
	@Override
	public void WriterDisconnect(){
		
		try {
			fs.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}		
	}

	@Override
	public void ReaderConnect() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void ReaderDisconnect() {
		// TODO Auto-generated method stub
		
	}

	/*
	 * create file and store data to it
	 */
	@Override
	public void Write(String data,String id) {
		
		// Create a new file and write data to it.
		this.path=this.path+"/"+this.keywords+"/file"+id;
		Path pth = new Path(this.path);
	    try {
			FSDataOutputStream out = fs.create(pth);
			fs.setPermission(pth,new FsPermission("777"));
			out.writeBytes(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	@Override
	public String Read() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void mkdir(String path,String keywords) throws IOException{
		
		//set path where to create directory
		path=path+"/"+keywords;
		Path pth=new Path(path);
		if(!fs.exists(pth))
			fs.mkdirs(pth);
	}
	
//	//-------------------------------------------------------------------------
//	//			Accessors
//	//-------------------------------------------------------------------------
//	public void setConfiguration(){
//		
//	}
//	public void getConfiguration(){
//		
//	}
//	
//	public void setPath(){
//		
//	}
//	public void getPath(){
//		
//	}
//	
//		public void setkeywords(){
//		
//	}
//	public void getkeywords(){
//		
//	}
	
	//--------------------------------------------------------------------------
	//			Data Member
	//--------------------------------------------------------------------------
	String path;								//get a path to read or to write in
	String keywords;							//save data under a folder named value(search keywords)
	Configuration conf;							//set default configuration of hadoop
	FileSystem fs;								//manipulate files in hdfs
	@Override
	public void Write(String data) {
		// TODO Auto-generated method stub
		
	}
}
