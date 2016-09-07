package spark;


import java.util.Set;

import org.apache.spark.api.java.function.Function;


public class NegativeScoreFunction implements Function<DataFields,DataFields> {
	private static final long serialVersionUID = 1L;
	//@Override
	public DataFields call(DataFields datafields){
		System.out.println("=============NegativeFunctionBegin=============");
		
		String text = datafields.getText();
		Set<String> negWords = NegativeWords.getWords();
		String[] words = text.split(" ");
        int numWords = words.length;
        int numNegWords = 0;
        for (String word : words)
        {
            if (negWords.contains(word))
                numNegWords++;
        }
		System.out.println(numNegWords);
		System.out.println("=============NegativeFunctionEnd=============");
		
		datafields.setNegativeScore((float) numNegWords / numWords);
        return datafields;
            
	
	}
}

//package spark;
//
//
//import java.util.Set;
//
//
//import org.apache.spark.api.java.function.PairFunction;
//
//import scala.Tuple2;
//
//public class NegativeScoreFunction implements PairFunction<Tuple2<Long,String>
//																	,Tuple2<Long,String>,Float> {
//	private static final long serialVersionUID = 1L;
//	//@Override
//	public Tuple2<Tuple2<Long,String>,Float> call(Tuple2<Long,String> tweet){
//		System.out.println("=============NegativeFunctionBegin=============");
//		String text = tweet._2;
//		Set<String> negWords = NegativeWords.getWords();
//		String[] words = text.split(" ");
//        int numWords = words.length;
//        int numNegWords = 0;
//        for (String word : words)
//        {
//            if (negWords.contains(word))
//                numNegWords++;
//        }
//		System.out.println(numNegWords);
//		System.out.println("=============NegativeFunctionEnd=============");
//
//        return new Tuple2<Tuple2<Long, String>, Float>(
//            new Tuple2<Long, String>(tweet._1(), tweet._2()),
//            (float) numNegWords / numWords
//        );
//	
//	}
//}
