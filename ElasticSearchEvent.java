//package es;
//
//import java.io.File;
//import java.io.FileReader;
//import java.io.InputStream;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//
//import org.elasticsearch.action.index.IndexResponse;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.client.IndicesAdminClient;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
//
//
//
//public class ElasticSearchEvent {
//
//	public static void main(String[] args) throws UnknownHostException {
//		
//		Client client;
//		String id="lkl";
//		String json="" ;
//
//		//set the client
//		Settings settings = Settings.settingsBuilder()
//					.put("cluster.name","elasticsearch").build();
//			
//			client = TransportClient.builder().settings(settings)
//					.build().addTransportAddress(
//							new InetSocketTransportAddress(
//							InetAddress.getByName("127.0.0.1"), 9300
//							));
//			
//			//creating index if doesn't exist
//			IndicesAdminClient indicesAdminClient = client.admin().indices();
//			if(!indicesAdminClient.prepareExists("twitter").execute().actionGet().isExists()){
//				indicesAdminClient.prepareCreate("twitter").get();
//			}
//			
//			//	JsonParser parser = new JsonParser();			 
//	        try {
//	 
//	        	JSONParser parser = new JSONParser();
//
//	            File directory = new File("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/JsonOutput");
//
//	            //get all the files from a directory
//	            File[] fList = directory.listFiles();
//
//	            for (File file : fList){
//		        	try {
//		                System.out.println("Reading JSON file from Java program");
//		                FileReader fileReader = new FileReader("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/JsonOutput/surface/"+file.getName());
//		                JSONObject js = (JSONObject) parser.parse(fileReader);
//
//		                 id = js.get("id").toString();
//		                 System.out.println("id : "+id);
//		                 json=js.toString();
//		                 System.out.println("jsonText : "+json);
//		                 
//		     			//index a document
//		     	        IndexResponse response = client.prepareIndex("twitter", "tweet", id)
//		     	                .setSource(json)
//		     	                .get();	  
//		                 
//		            } catch (Exception ex) {
//		                ex.printStackTrace();
//		            }
//	                System.out.println(file.getName());
//
//	            }
//	 
//	        } catch (Exception e) {
//	            e.printStackTrace();
//	        }
//      
//			
//	        //sutting down
//			client.close();
//
//
//			
//		
//	}
//
//}
