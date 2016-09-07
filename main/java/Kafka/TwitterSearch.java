package kafka;

import core.base.Properties;
import core.dataContainer.KafkaContainer;
import core.dataSource.TwitterSearchSource;

public class TwitterSearch {
	
	  public void collect() {    
		 

		// Set kafka parameters.

		    KafkaContainer kafka = new KafkaContainer("test1");
		    kafka.setBrokers("127.0.1.1:6667");
		    
		  //Set twitter Api parameters
		    TwitterSearchSource twitter = new TwitterSearchSource(twitterCostumerKey,
		    													twitterCustomerSecret,
		    													twitterAccessToken,
		    													twitterAccessTookenSecret,
		    													SinceDate,
		    													kafka);
		    
		 // Start collecting
		    twitter.connect();
		    twitter.collect();
	}
	  
	  //-----------------------------------------------------------------------------
	  //			Data Member
	  //-----------------------------------------------------------------------------
	  private static final String twitterCostumerKey = Properties.getString("twitter.Customerkey","twitter");
	  private static final String twitterCustomerSecret = Properties.getString("twitter.CustomerSecret","twitter");
	  private static final String twitterAccessToken = Properties.getString("twitter.AccessToken","twitter");
	  private static final String twitterAccessTookenSecret = Properties.getString("twitter.AccessTookenSecret","twitter");
	  private static final String SinceDate = Properties.getString("twitter.search.SinceDate", "twitter");

}
