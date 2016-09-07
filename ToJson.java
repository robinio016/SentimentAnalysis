//package spark;
//
//import java.io.IOException;
//
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.streaming.Time;
//import org.json.JSONObject;
//
//
//import scala.Tuple10;
///**
// * Transform rddfile to a clean version of json to be stored later on elasticsearch
// *
// */
//public class ToJson implements Function2<JavaRDD<Tuple10<Long,String,String,String,String,String,String,Float,Float,String>>
//,Time,Void> {
//	private static final long serialVersionUID = 1L;
//	//Override
//	public Void call(JavaRDD<Tuple10<Long,String,String,String,String,String,String,Float,Float,String>> rdd, Time time){
//		
//		rdd.foreach(new TransformRDDToJson());
//		
//		return null;		
//	}
//}
//
//class TransformRDDToJson 
//	implements VoidFunction<Tuple10<Long, String,String,String,String,String,String, Float, Float, String>>{
//	private static final long serialVersionUID = 1L;
//	//Override
//	public void call(Tuple10<Long, String,String,String,String,String,String, Float, Float, String> rddFile){
//		
//		long id=rddFile._1();
//		String text=rddFile._2();
//		String origin=rddFile._3();
//		String createdAt=rddFile._4();
//		String location=rddFile._5();
//		int retweetedCount=Integer.parseInt(rddFile._6());
//		int favoriteCount=Integer.parseInt(rddFile._7());;
//		float posScore=rddFile._8();
//		float negScore=rddFile._9();
//		String score=rddFile._10();
//		
//		//start converting
//JSONObject transformedData = new JSONObject();
//		
//		try{
//
//		      // Setting id,origin
//		      transformedData.put("id", id);  
//		      transformedData.put("origin", origin);
//		      transformedData.put("createdDate", createdAt);
//
//		      // Setting Text,and score
//		      transformedData.put("text", text);
//		      transformedData.put("positiveScore",posScore); 
//		      transformedData.put("negativeScore",negScore); 
//		      transformedData.put("score",score); 
//
//		      // Setting lang
//		      transformedData.put("lang", "en"); 
//
//		      // Setting likes, shares
//		      transformedData.put("likes", favoriteCount) ;
//		      transformedData.put("shares", retweetedCount);
//		      
//
//		      // Getting location 
//		      transformedData.put("location", location);
//			
//		}catch(Exception e){
//			System.out.println("Error while Transforming Data : "+e.getMessage());
//		}
//		
//		//save JsonObject to file
//		try {
//			java.io.FileWriter file = new java.io.FileWriter("/home/amine/workspace/ProjetEte.Kafka/src/main/resources/JsonOutput/file"+Long.toString(id)+".json");
//			transformedData.write(file);
//			file.flush();
//			file.close();
//
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//	}
//}
