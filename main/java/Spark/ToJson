package spark;

import java.io.File;
import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.json.JSONObject;

import core.base.Properties;
import core.dataContainer.AbstractContainer;
import core.dataContainer.AbstractFactory;
import core.dataContainer.ElasticSearchContainer;


public class ToJsonVoidFun implements
		VoidFunction<JavaRDD<DataFields>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2083861218847475061L;

	public void call(JavaRDD<DataFields> rdd)
			throws Exception {
		rdd.foreach(new TransformRDDToJson());
	}

	class TransformRDDToJson implements VoidFunction<DataFields> {
		private static final long serialVersionUID = 1L;

		// Override
		public void call(DataFields datafields) {

			long id = datafields.getId();
			String text = datafields.getText();
			String origin = datafields.getOrigin();
			String createdAt = datafields.getCreatedAt();
			String location = datafields.getLocation();
			int retweetedCount = datafields.getRetweetedCount();
			int favoriteCount = datafields.getFavoriteCount();
			float posScore = datafields.getPositiveScore();
			float negScore = datafields.getNegativeScore();
			String score = datafields.getScore();

			// start converting
			JSONObject transformedData = new JSONObject();

			try {

				// Setting id,origin
				transformedData.put("id", id);
				transformedData.put("origin", origin);
				transformedData.put("createdDate", createdAt);
				
				//this value for testing mapping functionalities in elasticsearch (to adjust later)
				transformedData.put("created_at", createdAt);


				// Setting Text,and score
				transformedData.put("text", text);
				transformedData.put("positiveScore", posScore);
				transformedData.put("negativeScore", negScore);
				transformedData.put("score", score);

				// Setting lang
				transformedData.put("lang", "en");

				// Setting likes, shares
				transformedData.put("likes", favoriteCount);
				transformedData.put("shares", retweetedCount);

				// Getting location
				transformedData.put("location", location);

			} catch (Exception e) {
				System.out.println("Error while Transforming Data : " + e.getMessage());
			}

			// save JsonObject to file
			try {
				/*
				 * setting newpath to directory (
				 */
				String newPath;
				newPath=Properties.getString("spark.saveJsonTo", "spark")
						+Properties.getString("twitter.keyword", "spark");
				
				File dossier = new File(newPath);
				if(!dossier.exists()){
					dossier.mkdir();
				}
				//------------------------------------------------------------------
				//			use of abstract factory
				//-------------------------------------------------------------------
//				AbstractFactory factory=AbstractFactory.getFactory();
//				AbstractContainer elasticSearchContainer = factory.createContainer();
//				
//				elasticSearchContainer.WriterConnect();
////				elasticSearchContainer.setIndex("usaaaa");
////				elasticSearchContainer.setType(keywords);
////				elasticSearchContainer.createIndex();
//				//esContainer.saveFilesToElasticSearch(pathToJson);
//				elasticSearchContainer.Write(transformedData.toString());
				
				//store in hdfs
				AbstractFactory fact=AbstractFactory.getFactory();
				AbstractContainer hdfs = fact.createContainer();
				hdfs.WriterConnect();
				hdfs.Write(transformedData.toString(),Long.toString(id));
				hdfs.WriterDisconnect();
				
				//------------------------------------------------------------------
				
				
//				//new java.io.File(Properties.getString("spark.saveJsonTo", "spark")
////						+"/"+
////						Properties.getString("twitter.keyword", "twitter")).mkdir();
//				java.io.FileWriter file = new java.io.FileWriter(
//						newPath
//						+"/file"
//						+ Long.toString(id)
//						+ ".json");
//				
//				/*
//				 * save data to elasticSearch
//				 */
//				String keywords =Properties.getString("twitter.keyword","es");
//				String pathToJson =Properties.getString("elasticSearch.toElasticSearchFrom", "es")
//									+"/"
//									+keywords
//									+"/";
//				System.out.println("++---***////333666998552114477\\\\"+keywords);
//				ElasticSearchContainer esContainer = new ElasticSearchContainer();
//				esContainer.WriterConnect();
//				esContainer.setIndex("usaaaa");
//				esContainer.setType(keywords);
//				esContainer.createIndex();
//				//esContainer.saveFilesToElasticSearch(pathToJson);
//				esContainer.Write(transformedData.toString());
//				//////////////////////////////////////////////////////////////////////////
//				
//				transformedData.write(file);
//				file.flush();
//				file.close();

			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

}

//package spark;
//
//import java.io.IOException;
//
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.json.JSONObject;
//
//import scala.Tuple10;
//
//public class ToJsonVoidFun implements
//		VoidFunction<JavaRDD<Tuple10<Long, String, String, String, String, String, String, Float, Float, String>>> {
//
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = -2083861218847475061L;
//
//	public void call(JavaRDD<Tuple10<Long, String, String, String, String, String, String, Float, Float, String>> rdd)
//			throws Exception {
//		rdd.foreach(new TransformRDDToJson());
//	}
//
//	class TransformRDDToJson implements
//			VoidFunction<Tuple10<Long, String, String, String, String, String, String, Float, Float, String>> {
//		private static final long serialVersionUID = 1L;
//
//		// Override
//		public void call(Tuple10<Long, String, String, String, String, String, String, Float, Float, String> rddFile) {
//
//			long id = rddFile._1();
//			String text = rddFile._2();
//			String origin = rddFile._3();
//			String createdAt = rddFile._4();
//			String location = rddFile._5();
//			int retweetedCount = Integer.parseInt(rddFile._6());
//			int favoriteCount = Integer.parseInt(rddFile._7());
//			;
//			float posScore = rddFile._8();
//			float negScore = rddFile._9();
//			String score = rddFile._10();
//
//			// start converting
//			JSONObject transformedData = new JSONObject();
//
//			try {
//
//				// Setting id,origin
//				transformedData.put("id", id);
//				transformedData.put("origin", origin);
//				transformedData.put("createdDate", createdAt);
//
//				// Setting Text,and score
//				transformedData.put("text", text);
//				transformedData.put("positiveScore", posScore);
//				transformedData.put("negativeScore", negScore);
//				transformedData.put("score", score);
//
//				// Setting lang
//				transformedData.put("lang", "en");
//
//				// Setting likes, shares
//				transformedData.put("likes", favoriteCount);
//				transformedData.put("shares", retweetedCount);
//
//				// Getting location
//				transformedData.put("location", location);
//
//			} catch (Exception e) {
//				System.out.println("Error while Transforming Data : " + e.getMessage());
//			}
//
//			// save JsonObject to file
//			try {
//				java.io.FileWriter file = new java.io.FileWriter(
//						"/home/amine/workspace/ProjetEte.Kafka/src/main/resources/JsonOutput/file" + Long.toString(id)
//								+ ".json");
//				transformedData.write(file);
//				file.flush();
//				file.close();
//
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//
//		}
//	}
//
//}
