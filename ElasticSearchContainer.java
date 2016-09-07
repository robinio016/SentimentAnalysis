package core.dataContainer;

import java.io.File;
import java.net.InetAddress;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import core.base.Properties;
import core.base.Utils;

public class ElasticSearchContainer extends AbstractContainer{
	
	//---------------------------------------------------------------------
	// 		Constructor
	//---------------------------------------------------------------------
	/**
	 * default constructor
	 */
	public ElasticSearchContainer(){
		this.host=Properties.getString("elasticSearch.host", "es");
		this.port=Properties.getInt("elasticSearch.port", "es");
		this.clusterName=Properties.getString("elasticSearch.clusterName", "es");
		
	}
	
	/**
	 * Constructor
	 * @param host
	 * @param port number
	 * @param clusterName
	 */
	public ElasticSearchContainer(String host,Integer port,String clusterName){
		this.host=host;
		this.port=port;
		this.clusterName=clusterName;
		
	}
	
	
	
	@Override
	public void WriterConnect() {
		// set the client
		try{
			//set the client
			Settings settings = Settings.settingsBuilder()
						.put("cluster.name",this.clusterName).build();
				
				client = TransportClient.builder().settings(settings)
						.build().addTransportAddress(
								new InetSocketTransportAddress(
								InetAddress.getByName(this.host), this.port
								));
				setIndex("usaaaa");
				setType(Properties.getString("twitter.keyword", "spark"));
				createIndex();
			
		}catch(Exception e){
			System.out.print("Error while setting the elasticsearch client : "+e.getMessage());
		}
		
	}

	@Override
	public void WriterDisconnect() {
		// shutdown client
		client.close();
		
	}

	@Override
	public void ReaderConnect() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void ReaderDisconnect() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * write data to ES
	 */
	@Override
	public void Write(String data) {
		 client.prepareIndex(this.index, this.type)
          .setSource(data)
          .get();	
	}

	@Override
	public String Read() {
		// TODO Auto-generated method stub
		return null;
	}
	
	//creating index if not exist
	public void createIndex(){
		try{
			IndicesAdminClient indicesAdminClient = client.admin().indices();
			if(!indicesAdminClient.prepareExists(index).execute().actionGet().isExists()){
				indicesAdminClient.prepareCreate(index).get();
				}
		}catch(Exception e){
			System.out.println("error while creating index : "+e.getMessage());
		}
	}
	
	//write data to elasticsearch from directory
	public void saveFilesToElasticSearch(String path){
		try{
			System.out.println(path);
			//path="/home/amine/Bureau/surface";
			//save all json file of a given directory to ES
			File directory = new File(path);

	        //get all the files from a directory
	        File[] fList = directory.listFiles();
	        for (File file : fList){
	        	try {
	        		String data =Utils.getStringFromJsonFile(file);
	        		//save data to elasticsearch
	        		this.Write(data);
	            } catch (Exception ex) {
	                ex.printStackTrace();
	            }
	            System.out.println(file.getName());

	        }

	     }catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	//-------------------------------------------------------------------
	// 		Accessors
	//-------------------------------------------------------------------

	  public void setIndex(String index) {
	    this.index = index;
	  }

	  public void setType(String type) {
	    this.type = type;
	  }
	//-----------------------------------------------------------------------
	//			Data Member
	//------------------------------------------------------------------------
	
	private int port;							//elasticSearch port to use when manipulating
	private String host;						//host machine that runs elasticsearch services
	private String clusterName;
	private String index;						//elastic search index
	private String type;						//elastic search type
	Client client;								//client to manage connexion in elasticsearch
	@Override
	public void Write(String data, String id) {
		// TODO Auto-generated method stub
		
	}
}
