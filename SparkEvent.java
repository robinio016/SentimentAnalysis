//package spark;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//
//import scala.Tuple10;
//import scala.Tuple2;
//import scala.Tuple4;
//
//
//
///**
// * Data transformation with spark
// * Contains Spark function to clean Json file and to analyze data (sentiment)
// */
//public class SparkEvent {
//	
//	public static void main(String[] args){
//		
//		//Spark configuration
//		SparkConf conf = new SparkConf();
//		conf.setAppName("Twitter sentiment analyses");
//		conf.setMaster("local[6]");
//		JavaStreamingContext ssc = new JavaStreamingContext(conf,new Duration(2000));
//		
//		//match kafka topic with thread to work on it 
//		Map<String,Integer> topicMap = new HashMap<String,Integer>();
//		topicMap.put("test1", 2); 				//one thread working on topic test1		
//		
//		//******************************************************************
//		//maybe usful to learn from the beggining of the offset kafka
//		//***********************************************************************
////		Map<String,String> topicMaps = new HashMap<String,String>();
////		topicMaps.put("bootstrap.servers" , "tcb:6667");
////		topicMaps.put("schema.registry.url" ,"http://tcb:8081"); 
////		topicMaps.put("zookeeper.connect", "127..1.1:2181"); 
////		topicMaps.put("group.id" , "a");
////		topicMaps.put("auto.offset.reset" , "smallest");
////		Set<String> topic = new HashSet<String>();
////		topic.add("test1");
////		JavaPairInputDStream<String, String> messages =
////		KafkaUtils.createDirectStream(ssc,
////								String.class,
////								String.class,
////								StringDecoder.class,
////								StringDecoder.class,
////								topicMaps,
////								topic);
//		//***************************************************************************
//		
//		
//		/**
//		 * getting message by topic from kafka producer using spark-kafka-streaming
//		 * result [ (topic_1_Name,contenu_1),(topic_2_Name,contenu_2), ....]
//		 */
//		
//
//		JavaPairReceiverInputDStream<String,String> messages =
//				KafkaUtils.createStream(ssc,
//										"127.0.1.1:2181",
//										"GROUP3",
//										topicMap);
//	
//		//getting message contents
//		/**
//		 * result : contenu_1 , ...
//		 */
//        JavaDStream<String> json = messages.map(
//                new Function<Tuple2<String, String>, String>() {   
//                	/**
//					 * 
//					 */
//					private static final long serialVersionUID = 1L;
//					
//					//@Override
//                    public String call(Tuple2<String, String> message) {
//                    	System.out.println("start mapping");
//                        return message._2();
//                    }
//                }
//            );
//        System.out.println("**********************************************");	
//
//		System.out.println("++ " + json.count());
//
//        
//        //getting  tweets that has an id and text different than null mapped together
//        /**
//         * result [(id1_1,text1_1),(id2_1,text2_1),....,(id1_2,text1_2),...]
//         */
//        JavaPairDStream<Long, String> tweets = json.mapToPair(
//        		new TwitterFilterFunction()
//        		);
//        
//        
//        //filter null tweet : non-english or malformed tweet may return null
//        /**
//         * result [(id1_1,text1_1),(id2_1,text2_1),....,(id1_2,text1_2),...]
//         */
//        JavaPairDStream<Long,String> filtered = tweets.filter(
//        		new Function<Tuple2<Long,String>,Boolean>(){
//        			private static final long serialVersionUID = 1L;
//        				//@Override
//        				public Boolean call(Tuple2<Long,String> tweet){
//        					
//        					return  tweet != null;
//        				}
//        		});
//		
//        
//        //filter text
//        /**
//         * result [(id1_1,text1_1),(id2_1,text2_1),....,(id1_2,text1_2),...]
//         * more filter text
//         */
//        JavaDStream<Tuple2<Long,String>> tweetsFiltered = filtered.map(
//        		new TextFilterFunction());
//        
//        
//        //discard commun words
//        /**
//         * result [(id1_1,text1_1),(id2_1,text2_1),....,(id1_2,text1_2),...]
//         * more filter text
//         */
//    	tweetsFiltered = tweetsFiltered.map(
//    			new stemmingFunction());
//    	
//    	
//    	// accord positive score to a given tweet
//    	/**
//         * result [(id1_1,text1_1,scorePos1_1),(id2_1,text2_1,scorePos2_1),....,(id1_2,text1_2,scorePos1_2),...]
//         */
//    	JavaPairDStream<Tuple2<Long,String>,Float> positiveTweets = tweetsFiltered
//    			.mapToPair(
//    				new PositiveScoreFunction()
//    			);
//    	
//    	//accordd negative score to a given tweet
//    	/**
//         * result [(id1_1,text1_1,scoreNeg1_1),(id2_1,text2_1,scoreNeg2_1),....,(id1_2,text1_2,scoreNeg1_2),...]
//         */
//    	JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets =
//                tweetsFiltered.mapToPair(new NegativeScoreFunction());
//    	
//    	//join results
//    	JavaPairDStream<Tuple2<Long,String>,Tuple2<Float,Float>> joined 
//    					= positiveTweets.join(negativeTweets);
//    	/**
//         * result [(id1_1,text1_1,scorePos1_1,scoreNeg2_1),(id2_1,text2_1,scorePos2_1,socreNeg2_1),....,(id1_2,text1_2,score1_2),...]
//         */
//    System.out.println("---------------------------------");	
//    	//group tweet score (negative and positve value) in the same tuple
//    	JavaDStream<Tuple4<Long, String, Float, Float>> scoredTweets =
//                joined.map(new Function<Tuple2<Tuple2<Long, String>,
//                                               Tuple2<Float, Float>>,
//                                        Tuple4<Long, String, Float, Float>>() {
//                	private static final long serialVersionUID = 1L;
//   
//                //@Override
//                public Tuple4<Long, String, Float, Float> call(
//                    Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>> tweet)
//                {
//                    return new Tuple4<Long, String, Float, Float>(
//                        tweet._1()._1(),
//                        tweet._1()._2(),
//                        tweet._2()._1(),
//                        tweet._2()._2());
//                }
//            });
//    	
//    	//add final remaks positive or negative tweet by combaring value
//    	JavaDStream<Tuple10<Long,String,String,String,String,String,String,Float,Float,String>> result = scoredTweets.map(
//    			new ScoreTweetsFunction()
//    			);
//    	
////    	//write to disk (later, i have to save it to elasticsearch)
////    	result.foreachRDD(new FileWriter());
//    	
//    	//to Json
//    	result.foreachRDD(new ToJsonVoidFun());
//    	
//    	
//    	//starting context
//    	ssc.start();
//        ssc.awaitTermination();
//	}
//
//}
