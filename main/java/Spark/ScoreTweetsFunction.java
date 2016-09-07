package spark;

import org.apache.spark.api.java.function.Function;



/*
 * decide if sentiment it is negative or positive by comparing negative and positive score value
 */
public class ScoreTweetsFunction implements Function<DataFields,DataFields>{
	private static final long serialVersionUID = 1L;
	//Override
	public DataFields call(DataFields datafields){
		
		if(datafields.getPositiveScore() >= datafields.getNegativeScore()){
			datafields.setScore("positive");
		}else{
			datafields.setScore("negative");
		}

		
		return datafields;
	}
}


//package spark;
//
//import org.apache.spark.api.java.function.Function;
//
//import scala.Tuple10;
//import scala.Tuple4;
//import scala.Tuple5;
//
///*
// * decide if sentiment it is negative or positive by comparing negative and positive score value
// */
//public class ScoreTweetsFunction implements Function<Tuple4<Long,String,Float,Float>,
//											Tuple10<Long,String,String,String,String,String,String,Float,Float,String>>{
//	private static final long serialVersionUID = 1L;
//	//Override
//	public Tuple10<Long,String,String,String,String,String,String,Float,Float,String> call(Tuple4<Long,String,Float,Float> tweet){
//		
//		String score;
//		if(tweet._3() >= tweet._4()){
//			score = "positive";
//		}else{
//			score="negative";
//		}
//		
//		String text="null";
//		String origin="null";
//		String createdAt="null";
//		String location="null";
//		String retweetedCount="0";
//		String favoriteCount="0";
//		
//		System.out.println("******/////////////*************////////////***********");
//		String[] otherInformation = tweet._2().split(" \\| ");
//		for(String word : otherInformation){
//			System.out.println(word);
//		}
//		text=otherInformation[0];
//		origin=otherInformation[1];
//		createdAt=otherInformation[2];
//		location=otherInformation[3];
//		retweetedCount=otherInformation[4];
//		favoriteCount=otherInformation[5];
//		
//		return new Tuple10<Long,String,String,String,String,String,String,Float,Float,String>(
//				tweet._1(),
//				text,
//				origin,
//				createdAt,
//				location,
//				retweetedCount,
//				favoriteCount,
//				tweet._3(),
//				tweet._4(),
//				score
//				);
//	}
//}
