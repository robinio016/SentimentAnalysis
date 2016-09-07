package spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import core.base.Properties;
import scala.Tuple2;



/**
 * Data transformation with spark
 * Contains Spark function to clean Json file and to analyze data (sentiment)
 */
public class SparkEventMain {
	
	public static void main(String[] args){
				
		//Spark configuration
		SparkConf conf = new SparkConf();
		conf.setAppName(Properties.getString("spark.appName", "spark"));
		conf.setMaster(Properties.getString("spark.master", "spark"));
		JavaStreamingContext ssc = new JavaStreamingContext(conf,new Duration(2000));
		
		//match kafka topic with thread to work on it 
		Map<String,Integer> topicMap = new HashMap<String,Integer>();
		topicMap.put(Properties.getString("spark.kafka_topics","spark")
				,Properties.getInt("spark.kafka_parallelization", "spark")); 				//one thread working on topic test1		
		
		
		/**
		 * getting message by topic from kafka producer using spark-kafka-streaming
		 * result [ (topic_1_Name,contenu_1),(topic_2_Name,contenu_2), ....]
		 */
		

		JavaPairReceiverInputDStream<String,String> messages =
				KafkaUtils.createStream(ssc,
										Properties.getString("zookeeper.zkhost", "spark"),
										Properties.getString("spark.kafka_groups", "spark"),
										topicMap);
	
		//getting message contents
		/**
		 * result : contenu_1 , ...
		 */
        JavaDStream<String> json = messages.map(
                new Function<Tuple2<String, String>, String>() {   
                	/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
					
					//@Override
                    public String call(Tuple2<String, String> message) {
                    	System.out.println("start mapping");
                        return message._2();
                    }
                }
            );

        
        //getting  tweets that has an id and text different than null mapped together


		JavaDStream<DataFields> tweets = json.map(
        		new TwitterFilterFunction()
        		);
        
        
        //filter null tweet : non-english or malformed tweet may return null
        /**
         * result [(id1_1,text1_1),(id2_1,text2_1),....,(id1_2,text1_2),...]
         */
        JavaDStream<DataFields> filtered = tweets.filter(
        		new Function<DataFields,Boolean>(){
        			private static final long serialVersionUID = 1L;
        				//@Override
        				public Boolean call(DataFields datafields){
        					
        					return  datafields != null;
        				}
        		});
		
        
        //filter text
        /**
         * result [(id1_1,text1_1),(id2_1,text2_1),....,(id1_2,text1_2),...]
         * more filter text
         */
        JavaDStream<DataFields> tweetsFiltered = filtered.map(
        		new TextFilterFunction());
        
        
        //discard commun words
        /**
         * result [(id1_1,text1_1),(id2_1,text2_1),....,(id1_2,text1_2),...]
         * more filter text
         */
    	tweetsFiltered = tweetsFiltered.map(
    			new stemmingFunction());
    	
    	
    	// accord positive score to a given tweet
    	/**
         * result [(id1_1,text1_1,scorePos1_1),(id2_1,text2_1,scorePos2_1),....,(id1_2,text1_2,scorePos1_2),...]
         */
    	JavaDStream<DataFields> positiveTweets = tweetsFiltered
    			.map(
    				new PositiveScoreFunction()
    			);
    	
    	//accordd negative score to a given tweet
    	/**
         * result [(id1_1,text1_1,scoreNeg1_1),(id2_1,text2_1,scoreNeg2_1),....,(id1_2,text1_2,scoreNeg1_2),...]
         */
    	JavaDStream<DataFields> negativeTweets =
    			positiveTweets.map(new NegativeScoreFunction());
    	
    System.out.println("---------------------------------");	

    	
    	//add final remaks positive or negative tweet by combaring value
    	JavaDStream<DataFields> result = negativeTweets.map(
    			new ScoreTweetsFunction()
    			);
    	
//    	//write to disk (later, i have to save it to elasticsearch)
//    	result.foreachRDD(new FileWriter());
    	
    	//to Json
    	result.foreachRDD(new ToJsonVoidFun());
    	
    	
    	//starting context
    	ssc.start();
        ssc.awaitTermination();
	}

}
