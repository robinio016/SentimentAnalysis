package core.dataSource;

import core.dataContainer.AbstractContainer;
import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * twitter data using twitter streaming api
 */
public class TwitterStreamSource extends AbstractTwitterSource {
	
	//----------------------------------------------------------
	//   constructor
	//----------------------------------------------------------
	 /**
	   * Constructs a TwitterSearchSource from the developer account
	   * @param consumerKey
	   * @param consumerSecret
	   * @param oAuthAccessToken
	   * @param oAuthAccessTokenSecret
	   * @param sinceDate                   the Begin date of Tweets to retrieve
	   * @param container
	   */
	  public TwitterStreamSource(String consumerKey,
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
					
					this.query = new FilterQuery();
	  }
	  
	  //-------------------------------------------------------------------
	  //   Implementation
	  //-------------------------------------------------------------------
	  /**
	   * Connect
	   */
	  public void connect(){
		  TwitterStream connection = new TwitterStreamFactory(getConfiguration().build()).getInstance();
		    setConnection(connection);	
	  }
	  
	  /**
	   * retrieving data from twitter and storing it in the data container
	   */
	  public void collect(){
		 try{
			 setQuery();
			 execute();
		 }
		 catch(Exception e){
			 System.out.println("Collect Error : "+e.getMessage());
		 }
		  
		  
	  }
	  /**
	   * set the query 
	   */
	  private void setQuery(){
		  FilterQuery q;
		    if (getKeyWords().isEmpty()) {
		      System.out.println("KeyWords need to be set first !!!");
		    } else {
		      q = new FilterQuery();
		      q.track(getKeyWords());
		      this.query = q;
		    }
	  }
	  
	  /**
	   * Getting tweets and storage to data container
	   */
	  private void execute() {
	    // connectong to data container in write mode
	    getContainer().WriterConnect();

	    // Creating the twitter stream listener that will listen for new tweets
	    StatusListener listener = createStatusListner();
	    ((TwitterStream)getConnection()).addListener(listener);
	    ((TwitterStream)getConnection()).filter(this.query);    
	  }
	  
	//Creating Tweeter stream listener that listens to new tweets
	  private StatusListener createStatusListner() {
	    StatusListener listener = new StatusListener() {
	      public void onStatus(Status status) {
	        saveToContainer(status);  
	      }

	      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	      public void onScrubGeo(long userId, long upToStatusId) {}
	      public void onException(Exception ex) {
	        ((TwitterStream)getConnection()).shutdown();
	        getContainer().WriterDisconnect();
	      }
	      public void onStallWarning(StallWarning arg0) {}
	    };

	    return listener;
	  }
	  //--------------------------------------------------------------------
	  //   Data Member
	  //--------------------------------------------------------------------
	  private FilterQuery query;							//twitter stream query

}
