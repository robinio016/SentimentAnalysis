package spark;

import java.util.Set;

import org.apache.spark.api.java.function.Function;


public class PositiveScoreFunction implements Function<DataFields,DataFields>{
	private static final long serialVersionUID = 1L;
	//Override
	public DataFields call(DataFields datafields){
		System.out.println("-+-+-+-+--+-+PositveFunctionBegin-+-+-+-+-+-+-+-+");
		 String text = datafields.getText();
		 Set<String> posWords = PositiveWords.getWords();
		
		 // divide text into words
		 String[] words = text.split(" ");
		 int numWords=words.length;
		 int numPosWords = 0;
		 
		 for(String word : words){
			 if(posWords.contains(word)){
				 numPosWords ++;
			 }
		 }
			System.out.println(numPosWords);
			System.out.println("-+-+-+-+--+-+PositveFunctionEnd+-+-+-+-+-+-+-+");
			
			datafields.setPositiveScore((float) numPosWords / numWords);
		 return datafields;
		 
	}
}

//package spark;
//
//import java.util.Set;
//
//import org.apache.spark.api.java.function.PairFunction;
//
//import scala.Tuple2;
//
//public class PositiveScoreFunction implements PairFunction<Tuple2<Long,String>,
//															Tuple2<Long,String>,Float>{
//	private static final long serialVersionUID = 1L;
//	//Override
//	public Tuple2<Tuple2<Long,String>,Float> call(Tuple2<Long,String> tweet){
//		System.out.println("-+-+-+-+--+-+PositveFunctionBegin-+-+-+-+-+-+-+-+");
//		 String text = tweet._2;
//		 Set<String> posWords = PositiveWords.getWords();
//		
//		 // divide text into words
//		 String[] words = text.split(" ");
//		 int numWords=words.length;
//		 int numPosWords = 0;
//		 
//		 for(String word : words){
//			 if(posWords.contains(word)){
//				 numPosWords ++;
//			 }
//		 }
//			System.out.println(numPosWords);
//			System.out.println("-+-+-+-+--+-+PositveFunctionEnd+-+-+-+-+-+-+-+");
//		 return new Tuple2<Tuple2<Long, String>, Float>(
//		            new Tuple2<Long, String>(tweet._1(), tweet._2()),
//		            (float) numPosWords / numWords
//		            );
//		 
//	}
//}
