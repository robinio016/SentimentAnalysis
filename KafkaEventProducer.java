package kafka;


public class KafkaEventProducer {

	/*
	 * Main to start collect data from sources (here twitter)
	 */
	public static void main(String[] args){
		//twitter search
		Runnable twitterRunnable = new Runnable(){
					public void run()
					{
						TwitterSearch twitterSearch= new TwitterSearch();
						twitterSearch.collect();
					}
				};
				
				
				//Create Threads : (not important here because i just run one thread on twitter)
				
				Thread twitterThread = new Thread(twitterRunnable);
				
				//Start threads
				
				twitterThread.start();
		}
}
