package core.dataSource;

import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import core.base.Constants;
import core.base.Properties;
import core.dataContainer.AbstractContainer;


/*
 *  representation of tweeter  data source using twitter search api
 */
public class TwitterSearchSource extends AbstractTwitterSource {
	//-----------------------------------------------------------------
	//   constructor
	//-----------------------------------------------------------------
	  /**
	   * Constructs a TwitterSearchSource from the developer account
	   * @param consumerKey
	   * @param consumerSecret
	   * @param oAuthAccessToken
	   * @param oAuthAccessTokenSecret
	   * @param sinceDate                   the Begin date of Tweets to retrieve
	   * @param container
	   */
	  public TwitterSearchSource(String consumerKey,
              					 String consumerSecret,
              					 String oAuthAccessToken,
              					 String oAuthAccessTokenSecret,
              					 String sinceDate,
              					 AbstractContainer container){
					super(consumerKey,
						  consumerSecret,
						  oAuthAccessToken,
						  oAuthAccessTokenSecret,
						  container);
					
					this.sinceDate = sinceDate; 
					this.query = new Query();
	  }
	  
	  //----------------------------------------------------------------
	  //   Implementation
	  //-----------------------------------------------------------------
	  public void connect(){
		  try{
			  Twitter connection = new TwitterFactory(getConfiguration().build()).getInstance();
			  setConnection(connection);
		  }
		  catch(Exception e){
			  System.out.println(e.getMessage());
		  }
		  
	  }
	  
	  /**
	   * Collecting data from Twitter and storing in data container.
	   */
	  public void collect(){
		 try{
			 setKeyWord(Properties.getString("twitter.keyword", "twitter"));
			 setQuery();
			 execute();
		 }
		 catch(Exception e){
			 System.out.println("Query error : "+e.getMessage());
		 }
	  }
	  
	  /*
	   * set a query
	   */
	  private void setQuery(){
		  Query q;
		  
		if(getKeyWords().isEmpty())
		{
			//setKeyWord("tweet");
			System.out.println("keywords need to be set first !!!");
		}else {
		      q = new Query(getKeyWords());
		      // Setting the number of results to get for each iteration
		      q.setCount(Constants.QUERY_STEP);
		      // Setting the start date of tweets search
		      q.setSince(sinceDate);
		      this.query = q;
		      }
	  }
	  
	  /*
	   * execute query and retrieve tweets
	   */
	  public void execute(){
		  //connecting the container to write mode
		  QueryResult queryResult;
		  getContainer().WriterConnect();
		    // iterating queries until no results are found
		    do {
		      try {    
		        Thread.sleep(6000);

		        queryResult = ((Twitter)getConnection()).search(this.query);
		        List<Status> tweets = queryResult.getTweets();
		        for (Status tweet : tweets) {
		          saveToContainer(tweet);
		        }

		        this.query = queryResult.nextQuery();
		      } catch (TwitterException e) {
		        System.out.println("TwitterException : " + e.getMessage());
		      } catch (InterruptedException e) {
		    	  System.out.println("Interrupted Exception : " + e.getMessage());
		      }

		    } while (null != this.query);
		    
		    getContainer().WriterDisconnect();
	  }
	  
	  //----------------------------------------------------------------
	  // data member
	  //----------------------------------------------------------------
	  private String sinceDate;          // Starting date for tweets to retrieve
	  private Query  query;              // the twitter query that holds keywords


}
