package core.dataSource;

import core.dataContainer.AbstractContainer;


import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Status;
import twitter4j.TwitterBase;
import twitter4j.TwitterObjectFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * represents an abstraction for twitter source type data soure such
 *  TwitterSearchSource and TwitterStreamSource
 */
public abstract class AbstractTwitterSource extends AbstractSource{
	
	//-----------------------------------------------------------------
	//   Constructor
	//-----------------------------------------------------------------
	
	 /**
	   * Construct a Twitter type data source given a twitter developer account
	   * @param consumerKey               
	   * @param consumerSecret
	   * @param oAuthAccessToken
	   * @param oAuthAccessTokenSecret
	   * @param container
	 */
	public AbstractTwitterSource(String consumerKey
								,String consumerSecret
								,String oAuthAccessToken
								,String oAuthAccessTokenSecret
								,AbstractContainer container){
		
		super(container);
		
		SetTwitterConfiguration(consumerKey
				,consumerSecret
				,oAuthAccessToken
				,oAuthAccessTokenSecret);
	}
	/*
	 * set twitter configuration
	 */
	private void SetTwitterConfiguration(String consumerKey
								,String consumerSecret
								,String oAuthAccessToken
								,String oAuthAccessTokenSecret){
		
		 configuration = new ConfigurationBuilder();
		 configuration.setDebugEnabled(true);
	     configuration.setJSONStoreEnabled(true)
	      .setOAuthConsumerKey(consumerKey)
	         .setOAuthConsumerSecret(consumerSecret)
	         .setOAuthAccessToken(oAuthAccessToken)
	         .setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
	}
	
	/**
	 * save a tweet to dataContainer
	 * @param tweet
	 */
	protected void saveToContainer(Status tweet){
		
		JSONObject jsonTweet = null;
		
	    try {
	        jsonTweet = new JSONObject(TwitterObjectFactory.getRawJSON(tweet));

	        jsonTweet.put("origin", "twitter");
	        String jsonTextTweet = jsonTweet.toString();
	        System.out.println(jsonTextTweet);
	        getContainer().Write(jsonTweet.toString());
	      } catch (JSONException e) {
	        System.out.println("Error while saving tweet to conatiner " + e.getMessage());
	      }
		
	}
	
	//------------------------------------------------------------------------
	//   Abstraction
	//-----------------------------------------------------------------------
	public abstract void connect();
	
	public abstract void collect();
	
	//------------------------------------------------------------------------
	//   Accessor
	//------------------------------------------------------------------------
	  protected ConfigurationBuilder getConfiguration() {
		    return this.configuration;
		  }

		  protected TwitterBase getConnection() {
		    return this.connection;
		  }

		  protected void setConnection(TwitterBase connection) {
		    this.connection = connection;
		  }
	
	//------------------------------------------------------------------------
	//   Data members
	//------------------------------------------------------------------------
	private ConfigurationBuilder configuration;									//twitter developer account configuration
	private TwitterBase connection;												//twitter connection
}
