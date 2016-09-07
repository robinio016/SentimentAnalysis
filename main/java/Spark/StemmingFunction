package spark;

import java.util.List;

import org.apache.spark.api.java.function.Function;


/**
 * 
 * Discard commun words from a tweet
 */
public class stemmingFunction implements Function<DataFields, DataFields>{
	
	private static final long serialVersionUID = 1L;
	//Override
	public DataFields call(DataFields datafields){
		System.out.println("------------StemmingFunctionBegin---------");
		String text = datafields.getText();
		//get list of stop-words readed from stop-words in ressource file
		List<String> stopWords = StopWords.getWords();
		for(String word : stopWords){
			text = text.replaceAll("\\b" + word + "\\b", "");
		}
		datafields.setText(text);
		System.out.println(text);
		System.out.println("------------StemmingFunctionEND---------");
		return datafields;
	}
}


//package spark;
//
//import java.util.List;
//
//import org.apache.spark.api.java.function.Function;
//
//import scala.Tuple2;
//
///**
// * 
// * Discard commun words from a tweet
// */
//public class stemmingFunction implements Function<Tuple2<Long, String>, Tuple2<Long, String>>{
//	
//	private static final long serialVersionUID = 1L;
//	//Override
//	public Tuple2<Long, String> call(Tuple2<Long, String> tweet){
//		System.out.println("------------StemmingFunctionBegin---------");
//		String text = tweet._2();
//		//get list of stop-words readed from stop-words in ressource file
//		List<String> stopWords = StopWords.getWords();
//		for(String word : stopWords){
//			text = text.replaceAll("\\b" + word + "\\b", "");
//		}
//		System.out.println(text);
//		System.out.println("------------StemmingFunctionEND---------");
//		return new Tuple2<Long, String>(tweet._1(), text);
//	}
//}
