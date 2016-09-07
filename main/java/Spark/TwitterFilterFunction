package spark;

import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import core.base.Utils;


/**
 * getting English tweets with an id and text different than null
 * each tweet is mapped to his id
 * getting additionnal information to construt our json file later on
 * 
 * out : (id,text)
 * format text : text +"|" + origin +"|" + created +"|" + location + "|" +favoriteCount +"|" + retweetedCount 
 */
public class TwitterFilterFunction implements Function<String,DataFields>{

	
	private static final long serialVersionUID = 1L;
	
	//create a mapper in order to parse json from string, stream or file,...
	private final ObjectMapper mapper = new ObjectMapper();
	
	public DataFields call(String tweet) throws Exception {
		
		//instancier dataFields
		DataFields datafields =new DataFields();
		
		System.out.println(tweet);
		
		try{
			JsonNode root = mapper.readValue(tweet, JsonNode.class);			
			
			/*
			 * get fields
			 */
			
			//get only tweet in english
			if(root.get("lang")!=null 
									&& "en".equals(root.get("lang").textValue())){

				//getting number of people that share or favorite the tweet 
				if(root.get("retweet_count") != null && root.get("favorite_count") != null){

					datafields.setRetweetedCount(root.get("retweet_count").intValue());
					datafields.setFavoriteCount(root.get("favorite_count").intValue());
				}
				
				if(root.get("origin")!=null){

					datafields.setOrigin(root.get("origin").textValue());
				}
				
				if(root.get("created_at")!=null){

					datafields.setCreatedAt(
							Utils.ToFormatDate(
									(root.get("created_at").textValue())
											));
				}
				
				if(root.get("user").get("location") != null){
					datafields.setLocation(root.get("user").get("location").textValue());
				}
				//getting text value of the tweet
				if(root.get("id")!=null && root.get("text")!=null){
					datafields.setId(root.get("id").longValue());
					datafields.setText(root.get("text").textValue());
					//System.out.println("!!!!!!!!!!!!"+datafields.getText());
					

					return datafields;
				}
				return null;
			}
			return null;
		}
		catch(Exception e){
			System.out.println("TwitterFilterFunction Error : "+e.getMessage());
		}
		return null;
	}
	
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;
//	//create a mapper in order to parse json from string, stream or file,...
//	private final ObjectMapper mapper = new ObjectMapper();
//	
//	public Tuple2<Long, String> call(String tweet) throws Exception {
//		System.out.println(tweet);
//		try{
//			JsonNode root = mapper.readValue(tweet, JsonNode.class);			
//			/*
//			 * fieds to extract from the original text
//			 */
//			long id=0;
//			String text="null";
//			String origin="null";
//			String createdAt="null";
//			String location="null";	
//			int retweetedCount=0;
//			int favoriteCount=0;
//			
//			/*
//			 * get fields
//			 */
//			
//			//get only tweet in english
//			if(root.get("lang")!=null 
//									&& "en".equals(root.get("lang").textValue())){
//				
//				//getting number of people that share or favorite the tweet 
//				if(root.get("retweet_count") != null && root.get("favorite_count") != null){
//					retweetedCount = root.get("retweet_count").intValue();
//					favoriteCount = root.get("favorite_count").intValue();
//				}
//				
//				if(root.get("origin")!=null){
//					origin = root.get("origin").textValue();
//				}
//				
////				//getting hashtag text value
////				if(root.get("entities").get("hashtags")!=null){
////					hashtag = root.get("entities").get("hashtags").get("text").textValue(); 
////				}
//				
//				if(root.get("user").get("location") != null){
//					location = root.get("user").get("location").textValue();
//				}
//				//getting text value of the tweet
//				if(root.get("id")!=null && root.get("text")!=null){
//					id = root.get("id").longValue();
//					System.out.println("id : "+id);
//					text =root.get("text").textValue();
//					System.out.println("!!!!!!!!!!!!"+text);
//					
//					//
//					text = text + " | " + origin + " | " + createdAt + " | " + 
//					location + " | " + favoriteCount + " | " +  retweetedCount  ;
//					System.out.println("!!!!!!!!!!!!"+text);
//					return new Tuple2<Long,String>(id,text);
//				}
//				return null;
//			}
//			return null;
//		}
//		catch(Exception e){
//			System.out.println("TwitterFilterFunction Error : "+e.getMessage());
//		}
//		return null;
//	}
//	

}
