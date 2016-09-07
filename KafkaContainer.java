package core.dataContainer;
//import util.properties packages
import java.util.Properties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;


import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/*
 * represantation of kafka producer and consumer
 * contains operation such connect to topic in kafka, write, read,...
 * runs with  Zookeeper 
 */

public class KafkaContainer extends AbstractContainer{
	
	//-----------------------------------------------
	//			Constructors
	//----------------------------------------------
	
	//@param
	//--------------------@topic---------------------
	public KafkaContainer(String topic)
	{
		this.topic = topic;
		this.position= new Integer(1); //
	}
	
	
	//@params
	  /**
	   * Constructs a kafka container given :
	   * @param topic           kafka topic name
	   * @param zookeeperHost   zookeeperHost (host:port)
	   * @param brokers         kafka brokers list
	   * @param groupId         kafka groupId
	   */
	public KafkaContainer(String topic
			,String ZookeeperHosts
			,String groupId){
		
		this.topic=topic;
		this.zookeeperHosts = ZookeeperHosts;
		this.groupId=groupId;
	}
	
//---------------------------------------------------------------------------
//			Implementation
//--------------------------------------------------------------------------

	
	/*
	 * connecting to kafka in writing mode (producer)
	 */
	@Override
	public void WriterConnect(){
		//Create connection configuration
		Properties props = new Properties();
		ProducerConfig prodConfig ;
	      
	      
	      //configuration Fahd
	      props.put("metadata.broker.list", this.brokers);
	      props.put("serializer.class", "kafka.serializer.StringEncoder");
	      //
	      props.put("message.max.bytes","" + (35 * 1024 * 1024));
	      props.put("replica.fetch.max.bytes","" + (35 * 1024 * 1024));
	      
	      prodConfig = new ProducerConfig(props);
	      
	      this.producer =new Producer<String,String>(prodConfig);
	}
	
	/*
	 * closing kafka producer
	 */
	@Override
	public void WriterDisconnect()
	{
		producer.close();
	}
	
	/*
	 * Connect in reading mode (consumer)
	 */
	@Override
	public void ReaderConnect()
	{
		//setting consumer configuration
		
		Properties props = new Properties();
	      props.put("bootstrap.servers", core.base.Properties.getString("Kafka.brokers", "kafka"));
	      props.put("group.id", this.groupId);
	      props.put("enable.auto.commit", "true");
	      props.put("auto.commit.interval.ms", "1000");
	      props.put("session.timeout.ms", "30000");
	      props.put("key.deserializer",          
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      
	      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap; 						//return a map that match topic(string) with its content(list kafkastream
	      KafkaStream<byte[], byte[]> stream;
	      
	      //creating the consumer
	      ConsumerConfig consumerConfig=new ConsumerConfig(props);
	      this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
	      
	      topicCountMap.put(this.topic,this.position);
	      
	      //getting the kafka topic message stream
	      consumerMap = consumerConnector.createMessageStreams(topicCountMap);				//how the information will pass to kafka
	      stream = consumerMap.get(this.topic).get(0);										//getting data
	      this.consumer = stream.iterator();
	}
	
	/**
	 * reader disconnect
	 */
	@Override
	public void ReaderDisconnect(){
		this.consumerConnector.shutdown();
	}

	/*
	 * add a message to producer
	 */
	@Override
	public void Write(String data){
		KeyedMessage<String,String> message = new KeyedMessage<String,String>(this.topic,data); //create a message (topic + data)
		this.producer.send(message);
	}
	
	/*
	 * read a message from a consumer
	 */
	@Override
	public String Read(){
		try{
			return this.consumer.hasNext() ? new String(this.consumer.next().message()) : null ;
		}
		catch(Exception e){
			System.out.println(e.getMessage());
			return null;
		}
	}

	
	 /**
	   * extract zookeeper port from string format 'host1:port,host2:port,..' 
	   * @param zkHosts
	   * @return int port
	   */
	private int getZKport(){
		return Integer.parseInt(this.zookeeperHosts.split(",")[0].split(":")[0]);
	}
	
	  /**
	   * extract zookeeper List hosts from string format 'host1:port,host2:port,..' 
	   * @param zkHosts
	   * @return List zkServers
	   */
	  private List<String> getZkServers() {
	    List<String> zkServers = new ArrayList<String>();
	    String[] listHost = this.zookeeperHosts.split(",");
	    
	    for(String host : listHost){
	      zkServers.add(host.split(":")[0]);
	    }
	    
	    return zkServers;
	  }

	
//-------------------------------------------------------------------
// ACCESSORS
//-------------------------------------------------------------------

	  public void setZookeeperHosts(String zookeeperHosts) {
	    this.zookeeperHosts = zookeeperHosts;
	  }

	  public void setGroupId(String groupId) {
	    this.groupId = groupId;
	  }

	  public void setPosition(int position) {
	    this.position = new Integer(position);
	  }
	  
	  public void setBrokers(String brokers) {
	    this.brokers = brokers;
	  }
	

	
//------------------------------------------------------------------------------------------
//   Data Member
//-------------------------------------------------------------------------------------------
	private String topic;		            			//topic name;
	private Integer position;	            			//position to read from;
	private String  zookeeperHosts;         			// zookeeper hosts list (separator is ',')
	private String brokers;                				// the list of kafka brokers (separator is ',')
	private String groupId; 	            			//group id in kafka topic
	
	private Producer<String,String> producer;			//kafka producer handler
	private ConsumerConnector consumerConnector;		//kafka consumer
	private ConsumerIterator<byte[], byte[]> consumer;	//kafka consumer handler
	@Override
	public void Write(String data, String id) {
		// TODO Auto-generated method stub
		
	}
}
